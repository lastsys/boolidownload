package booli

import java.io._
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, Year}

import booli.BooliJson.{Page, Sold}
import play.api.libs.json._
import play.api.http.Status.OK
import play.api.libs.ws.ning.NingWSClient

import scala.collection.immutable.ListMap
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.Random

object BooliJson {
  case class Page(totalCount: Int,
                  count: Int,
                  limit: Int,
                  offset: Int,
                  sold: Seq[Sold])

  case class Sold(location: Location,
                  listPrice: Option[Double],
                  livingArea: Option[Double],
                  additionalArea: Option[Double],
                  plotArea: Option[Double],
                  source: Source,
                  rooms: Option[Double],
                  published: LocalDateTime,
                  constructionYear: Option[Year],
                  objectType: String,
                  booliId: Int,
                  soldDate: LocalDate,
                  soldPrice: Double,
                  url: String,
                  floor: Option[Double])

  case class Location(address: Address,
                      position: Position,
                      namedAreas: Option[Seq[String]],
                      region: Region,
                      distance: Option[Distance])

  case class Address(streetAddress: String)

  case class Position(latitude: Double,
                      longitude: Double,
                      isApproximate: Option[Boolean])

  case class Region(municipalityName: String,
                    countyName: String)

  case class Source(name: String,
                    id: Int,
                    `type`: String,
                    url: String)

  case class Distance(ocean: Int)

  implicit val yearFormat = new Format[Year] {
    def reads(json: JsValue): JsResult[Year] =
      JsSuccess(java.time.Year.of(json.as[Int]))

    def writes(year: java.time.Year): JsValue =
      JsNumber(year.getValue)
  }
  implicit val dateTimeFormat = new Format[LocalDateTime] {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    def reads(json: JsValue): JsResult[LocalDateTime] =
      JsSuccess(LocalDateTime.parse(json.as[String], formatter))

    def writes(dateTime: LocalDateTime): JsValue =
      JsString(dateTime.format(formatter))
  }
  implicit val dateFormat = new Format[LocalDate] {
    def reads(json: JsValue): JsResult[LocalDate] =
      JsSuccess(LocalDate.parse(json.as[String]))

    def writes(date: LocalDate): JsValue =
      JsString(date.toString)
  }

  implicit val distanceFormat: Format[Distance] = Json.format[Distance]
  implicit val sourceFormat: Format[Source] = Json.format[Source]
  implicit val regionFormat: Format[Region] = Json.format[Region]
  implicit val positionFormat: Format[Position] = Json.format[Position]
  implicit val addressFormat: Format[Address] = Json.format[Address]
  implicit val locationFormat: Format[Location] = Json.format[Location]
  implicit val soldFormat: Format[Sold] = Json.format[Sold]
  implicit val pageFormat: Format[Page] = Json.format[Page]
}


object Download {
  case class Configuration(user: String,
                           token: String,
                           url: String,
                           pageSize: Int,
                           outputPath: String,
                           userAgent: String,
                           referrer: String,
                           csvSeparator: String)

  implicit val configurationReads: Reads[Configuration] = Json.format[Configuration]

  lazy val client = NingWSClient()
  lazy val configuration = {
    val source = scala.io.Source.fromFile("configuration.json")
    val text = try source.mkString finally source.close()
    val json = Json.parse(text)
    json.validate[Configuration].get
  }

  def main(args: Array[String]) {
    println(configuration)

    createOutputPath()

    //download()
    jsonToCsv()
  }

  def createOutputPath(): Unit = {
    val file = new File(configuration.outputPath)
    file.mkdirs()
  }

  def download(): Unit = {

    val totalCount = for {
      body ← get(offset = 0, limit = 1)
    } yield {
      println(body)
      val json = Json.parse(body)
      val count = (json \ "totalCount").get.toString.toInt
      println(s"About to fetch $count items.")
      count
    }

    totalCount.onFailure {
      case f ⇒
        println("Failed to get total count.")
        f.printStackTrace()
    }

    val task = for {
      count ← totalCount
    } yield {
      val pageCount = count / configuration.pageSize
      println(s"Reading $pageCount pages")
      val offsets = 0 to pageCount
      offsets.foreach {
        (i: Int) ⇒
          val path = f"${configuration.outputPath}/response$i%04d.json"
          if (!new java.io.File(path).exists) {
            val offset = i * configuration.pageSize
            // We need to be nice to the server we are calling.
            // If we do not await the result we will have *many*
            // simultaneous calls.
            val json = Await.result(
              get(offset, configuration.pageSize), 30 seconds)
            println(s"Writing $path")
            val file = new File(path)
            val bw = new BufferedWriter(
              new OutputStreamWriter(
                new FileOutputStream(file), "UTF-8"))
            bw.write(json.toString)
            bw.close()
          }
      }
      client.close()
    }
  }

  def get(offset: Int, limit: Int): Future[String] = {
    val r = Random.alphanumeric
    val unique = r.take(16).mkString
    val timestamp = System.currentTimeMillis / 1000
    val hash = {
      val md = java.security.MessageDigest.getInstance("SHA-1")
      val bytes = md.digest(
        (s"${configuration.user}" +
          s"$timestamp" +
          s"${configuration.token}" +
          s"$unique").getBytes("UTF-8"))
      bytes.map("%02x".format(_)).mkString
    }

    val request = client.url(configuration.url)
    val cmplxReq = request.withHeaders(
      "Accept" → "application/json",
      "User-Agent" → configuration.userAgent,
      "Referrer" → configuration.referrer)
      .withQueryString(
        "bbox" → "55,10,70,25",
        "maxpages" → 0.toString,
        "callerId" → configuration.user,
        "time" → timestamp.toString,
        "unique" → unique,
        "hash" → hash,
        "offset" → offset.toString,
        "limit" → limit.toString)

    val getResult = for {
      response ← cmplxReq.get()
    } yield {
      if (response.status != OK) {
        println(s"Got result with response ${response.status}")
      }
      response.body
    }

    getResult.onFailure {
      case f ⇒ f.printStackTrace()
    }

    getResult
  }

  def jsonToCsv(): Unit = {
    // Get all files.
    val dir = new File(configuration.outputPath)
    val files = if (dir.exists && dir.isDirectory) {
      dir.listFiles.filter(_.getName.endsWith(".json")).toSeq
    } else Seq[File]()

    val bw = new BufferedWriter(
      new OutputStreamWriter(
        new FileOutputStream("output.csv"), "UTF-8"))

    var firstRow = true

    files.foreach { f ⇒
      println(s"Converting ${f.getName}")
      val source = Source.fromFile(f)
      val text = try source.mkString finally source.close()
      Json.parse(text).validate[Page] match {
        case JsSuccess(p: Page, _) ⇒
          p.sold.foreach { s ⇒
            val row = buildRow(s)
            if (firstRow) {
              bw.write(row.keys.mkString(configuration.csvSeparator) + "\n")
              firstRow = false
            }
            bw.write(row.values.mkString(configuration.csvSeparator) + "\n")
          }
        case JsError(errors) ⇒ println(errors.mkString("\n"))
      }
    }

    bw.close()
  }

  def buildRow(sold: Sold): ListMap[String, String] = {
    implicit def convert[T](v: T): String = v.toString
    implicit def convertOpt[T](v: Option[T]): String = v match { case Some(w) ⇒ w.toString; case None ⇒ "" }

    ListMap[String, String](
      "id" → sold.booliId,
      "address" → sold.location.address.streetAddress,
      "distanceToOcean" → (sold.location.distance match {
        case Some(d) ⇒ d.ocean.toString
        case None ⇒ ""
      }),
      "latitude" → sold.location.position.latitude,
      "longitude" → sold.location.position.longitude,
      "approximateLocation" → sold.location.position.isApproximate,
      "municipality" → sold.location.region.municipalityName,
      "county" → sold.location.region.countyName,
      "constructionYear" → sold.constructionYear,
      "floor" → sold.floor,
      "livingArea" → sold.livingArea,
      "additionalArea" → sold.additionalArea,
      "objectType" → sold.objectType,
      "plotArea" → sold.plotArea,
      "rooms" → sold.rooms,
      "listPrice" → sold.listPrice,
      "soldPrice" → sold.soldPrice,
      "soldDate" → sold.soldDate,
      "published" → sold.published,
      "sourceName" → sold.source.name,
      "sourceType" → sold.source.`type`,
      "sourceUrl" → sold.source.url
    )
  }
}

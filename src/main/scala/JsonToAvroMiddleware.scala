import domain.CustomKafkaProducer
import domain.JsonAvroSerializer
import domain.SchemaJsonProtocol._
import domain.SchemaJsonObject

import java.util.concurrent.ConcurrentHashMap

import scala.io.StdIn
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.Done
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

import spray.json._

object JsonToAvroMiddleware {
  val schemaRegistryUrl = "http://localhost:8081"

  val producer = new CustomKafkaProducer()
  val serializer = new JsonAvroSerializer()
  var schemas: ConcurrentHashMap[String, String] = new ConcurrentHashMap

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  def main(args: Array[String]) {
    // server starts listening for events before fetching schemas
    // synchronization would be required in production environment
    fetchSchemas()

    // authentication would be required in production environment
    val route: Route =
      path("event") {
        post {
          formFields('name.as[String], 'json.as[String]) { (name, json) =>
            val emmision: Future[Done] = emitEvent(name, json)
            onComplete(emmision) { return }
          }
        }
      }
      path("update_schema") {
        post {
          formFields('schema.as[String]) { (schema) =>
            loadSchema(schema)
            onComplete(Future { Done }) { return }
          }
        }
      }

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    println("Server online at http://localhost:8080\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return

    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done

    producer.exit()
    system.terminate()
  }

  def emitEvent(name: String, json: String): Future[Done] = {
    try {
      producer.sendAvro(name, serializer.jsonToAvro(json, schemas.get(name)))
    } catch {
      // send invalid events to special topic; ignore malformed json; log unhandled exceptions
      // TODO: investigate relevant Throwable names
      // case _: <InvalidExeptions> => producer.sendString("invalid_events", json)
      // case _: <MalformedExceptions> => // skip
      case e: Throwable => // some logging
    }

    Future { Done }
  }

  def fetchSchemas() {
    val subjectsFuture: Future[HttpResponse] = Http().singleRequest(
      HttpRequest(uri = schemaRegistryUrl + "/subjects")
    )

    subjectsFuture.onComplete {
      case Success(response) => Unmarshal(response.entity).to[String].map { subjectString =>
        fetchSubjects(subjectString)
      }
      case Failure(_) => // some logging
    }
  }

  def fetchSubjects(subjectString: String) {
    val subjects = JsonParser(subjectString).convertTo[List[String]]

    subjects.foreach(subject => {
      val schemaFuture: Future[HttpResponse] = Http().singleRequest(
        HttpRequest(uri = schemaRegistryUrl + "/subjects/" + subject + "/versions/latest")
      )

      schemaFuture.onComplete {
        case Success(response) => Unmarshal(response.entity).to[String].map { schemaString =>
          loadSchema(schemaString)
        }
        case Failure(_) => // some logging
      }
    })
  }

  def loadSchema(schemaString: String) {
    val json = JsonParser(schemaString).convertTo[SchemaJsonObject]
    schemas.put(json.subject, json.schema)
  }
}

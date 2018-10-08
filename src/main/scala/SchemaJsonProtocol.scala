package domain

import spray.json._

case class SchemaJsonObject(subject: String, version: Int, id: Int, schema: String)

object SchemaJsonProtocol extends DefaultJsonProtocol {
  implicit val schemaFormat = jsonFormat4(SchemaJsonObject)
}

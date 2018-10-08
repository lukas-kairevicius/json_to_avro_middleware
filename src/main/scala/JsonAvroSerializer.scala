package domain

import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.io.DataInputStream

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io._

class JsonAvroSerializer() {
  def jsonToAvro(json: String, schemaStr: String): Array[Byte] = {
    val schema = new Schema.Parser().parse(schemaStr)
    val reader = new GenericDatumReader[GenericRecord](schema)
    val input = new ByteArrayInputStream(json.getBytes())
    val output = new ByteArrayOutputStream()
    val din = new DataInputStream(input)
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val decoder = DecoderFactory.get().jsonDecoder(schema, din)
    val encoder = EncoderFactory.get().binaryEncoder(output, null)

    val datum = reader.read(null, decoder)
    writer.write(datum, encoder)
    encoder.flush()

    return output.toByteArray()
  }
}

# json_to_avro_middleware

Barebones example scala app for serializing JSON to AVRO records and relaying them to Kafka topics.

[Confluent Schema Registry](https://www.confluent.io/confluent-schema-registry/),
[Akka](https://akka.io/),
[spray-json](https://github.com/spray/spray-json),
[Apache Avro](https://avro.apache.org/),
[Apache Kafka](https://kafka.apache.org/)

`JsonToAvroMiddleware` starts by fetching schemas from a local Schema Registry instance. Schemas are deserialized using `spray-json`. It then listens on `localhost:8080` for `event` requests, sending any valid ones to relevant Kafka topics as AVRO encoded records. Kafka topics should already exist and have schemas registered.

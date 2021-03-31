# json-to-avro-schema

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Description

Small utility class to generate an Avro schema from a JSON document based on Jackson library.

## Installation

```xml
<dependency>
  <groupId>com.github.mredjem</groupId>
  <artifactId>json-to-avro-schema</artifactId>
  <version>0.0.3</version>
</dependency>
```

## Usage

```java
JsonNode jsonNode = objectMapper.readTree(jsonContent);

Schema schema = new JsonToAvroSchema("com.github.mrm")
  .inferSchema("Test", jsonNode);

System.out.println(schema.toString(true));
```

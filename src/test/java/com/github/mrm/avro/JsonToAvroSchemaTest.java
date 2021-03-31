package com.github.mrm.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

class JsonToAvroSchemaTest {

  private static JsonNode testJson;
  private static JsonNode testJsonWithNulls;
  private static String testSchema;
  private static String testSchemaWithDoc;
  private static String testSchemaWithNulls;

  @BeforeAll
  public static void beforeAll() throws IOException {
    testJson = new ObjectMapper()
      .readTree(
        JsonToAvroSchemaTest
          .class
          .getResourceAsStream("/json-files/complex-json.json")
      );

    testJsonWithNulls = new ObjectMapper()
      .readTree(
        JsonToAvroSchemaTest
          .class
          .getResourceAsStream("/json-files/complex-json-with-nulls.json")
      );

    testSchema = IOUtils
      .toString(
        JsonToAvroSchemaTest
          .class
          .getResourceAsStream("/avro-schemas/complex-schema.avsc"),
        StandardCharsets.UTF_8
      );

    testSchemaWithDoc = IOUtils
      .toString(
        JsonToAvroSchemaTest
          .class
          .getResourceAsStream("/avro-schemas/complex-schema-with-doc.avsc"),
        StandardCharsets.UTF_8
      );

    testSchemaWithNulls = IOUtils
      .toString(
        JsonToAvroSchemaTest
          .class
          .getResourceAsStream("/avro-schemas/complex-schema-with-nulls.avsc"),
        StandardCharsets.UTF_8
      );
  }

  @Test
  void inferSchema() {
    Schema schema = new JsonToAvroSchema("com.github.mrm")
      .inferSchema("Test", testJson);

    Assertions.assertNotNull(schema);
    Assertions.assertEquals(testSchema, schema.toString(true));
  }

  @Test
  void inferSchemaWithDoc() {
    Schema schema = new JsonToAvroSchema("com.github.mrm", (name, isType) -> isType ? "Doc for type " + name : "Doc for field " + name)
      .inferSchema("Test", testJson);

    Assertions.assertNotNull(schema);
    Assertions.assertEquals(testSchemaWithDoc, schema.toString(true));
  }

  @Test
  void inferSchemaWithNulls() {
    Schema schema = new JsonToAvroSchema("com.github.mrm")
      .inferSchema("Test", testJsonWithNulls);

    Assertions.assertNotNull(schema);
    Assertions.assertEquals(testSchemaWithNulls, schema.toString(true));
  }

}

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

public class JsonToAvroTest {

  private static JsonNode testJson;
  private static String testSchema;
  private static String testSchemaWithDoc;

  @BeforeAll
  public static void beforeAll() throws IOException {
    testJson = new ObjectMapper()
      .readTree(
        JsonToAvroTest
          .class
          .getResourceAsStream("/json-files/complex-json.json")
      );

    testSchema = IOUtils
      .toString(
        JsonToAvroTest
          .class
          .getResourceAsStream("/avro-schemas/complex-schema.avsc"),
        StandardCharsets.UTF_8
      );

    testSchemaWithDoc = IOUtils
      .toString(
        JsonToAvroTest
          .class
          .getResourceAsStream("/avro-schemas/complex-schema-with-doc.avsc"),
        StandardCharsets.UTF_8
      );
  }

  @Test
  public void inferSchema() throws Exception {
    Schema schema = new JsonToAvro("com.github.mrm")
      .inferSchema("Test", testJson);

    Assertions.assertNotNull(schema);
    Assertions.assertEquals(testSchema, schema.toString(true));
  }

  @Test
  public void inferSchemaWithDoc() throws Exception {
    Schema schema = new JsonToAvro("com.github.mrm", (name, isType) -> isType ? "Doc for type " + name : "Doc for field " + name)
      .inferSchema("Test", testJson);

    Assertions.assertNotNull(schema);
    Assertions.assertEquals(testSchemaWithDoc, schema.toString(true));
  }

}

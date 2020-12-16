package com.github.mrm.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;

/**
 * Class to generate a consistent Avro schema from a JSON document based on Jackson library.
 *
 * @author mredjem
 */
public class JsonToAvro {

  private final String namespace;
  private final DocVisitor docVisitor;

  /**
   * Constructor for {@link JsonToAvro}.
   *
   * @param namespace the Avro schema namespace
   * @param docVisitor the {@link DocVisitor} implementation
   */
  public JsonToAvro(String namespace, DocVisitor docVisitor) {
    this.namespace = namespace;
    this.docVisitor = docVisitor;
  }

  /**
   * Constructor for {@link JsonToAvro}.
   *
   * @param namespace the Avro schema namespace
   */
  public JsonToAvro(String namespace) {
    this(namespace, null);
  }

  /**
   * Infer a reasonable Avro schema from a parsed JSON document.
   *
   * @param name the name for the root record
   * @param node the {@link JsonNode}
   * @return a {@link Schema}
   */
  public Schema inferSchema(String name, JsonNode node) {
    SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(name);

    if (docVisitor != null) builder = builder.doc(docVisitor.doc(name, true));

    SchemaBuilder.FieldAssembler<Schema> assembler = builder
      .namespace(namespace)
      .fields();

    return setSchemaFields(assembler, node).endRecord();
  }

  /**
   * Crawl the JSON document fields to update the underlying schema assembler.
   *
   * @param assembler the {@link SchemaBuilder.FieldAssembler}
   * @param node the {@link JsonNode}
   * @return an updated {@link SchemaBuilder.FieldAssembler}
   */
  private SchemaBuilder.FieldAssembler<Schema> setSchemaFields(SchemaBuilder.FieldAssembler<Schema> assembler, JsonNode node) {
    Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();

    while (iterator.hasNext()) {
      Map.Entry<String, JsonNode> entry = iterator.next();

      JsonNodeType type = entry.getValue().getNodeType();

      if (isScalarField(type)) {
        assembler = setScalarFields(assembler, entry);
      }
      else if (type == JsonNodeType.OBJECT) {
        SchemaBuilder.RecordBuilder<Schema> nestedType = SchemaBuilder
          .record(entry.getKey())
          .namespace(namespace);

        if (docVisitor != null) nestedType = nestedType.doc(docVisitor.doc(entry.getKey(), true));

        SchemaBuilder.FieldAssembler<Schema> objectAssembler = nestedType.fields();
        Schema nestedSchema = setSchemaFields(objectAssembler, entry.getValue()).endRecord();

        assembler = assembler.name(entry.getKey()).type(nestedSchema).noDefault();
      }
      else if (type == JsonNodeType.ARRAY) {
        SchemaBuilder.RecordBuilder<Schema> itemType = SchemaBuilder
          .array()
          .items()
          .record(entry.getKey())
          .namespace(namespace);

        if (docVisitor != null) itemType = itemType.doc(docVisitor.doc(entry.getKey(), true));

        SchemaBuilder.FieldAssembler<Schema> itemAssembler = itemType.fields();

        JsonNode itemNode = entry.getValue().elements().next();
        itemAssembler = setSchemaFields(itemAssembler, itemNode);

        assembler = assembler.name(entry.getKey()).type(itemAssembler.endRecord()).noDefault();
      }
    }

    return assembler;
  }

  /**
   * Checks whether the JSON node is a scalar or not.
   *
   * @param type the {@link JsonNodeType}
   * @return <code>true</code> if scalar, <code>false</code> otherwise
   */
  private boolean isScalarField(JsonNodeType type) {
    return EnumSet.of(
      JsonNodeType.STRING,
      JsonNodeType.BOOLEAN,
      JsonNodeType.NUMBER,
      JsonNodeType.BINARY,
      JsonNodeType.NULL
    ).contains(type);
  }

  /**
   * Generate Avro schema fields based on a JSON document scalar field.
   *
   * @param assembler the {@link SchemaBuilder.FieldAssembler}
   * @param entry the {@link Map.Entry} for the current node
   * @return a {@link SchemaBuilder.FieldAssembler}
   */
  private SchemaBuilder.FieldAssembler<Schema> setScalarFields(SchemaBuilder.FieldAssembler<Schema> assembler, Map.Entry<String, JsonNode> entry) {
    JsonNodeType nodeType = entry.getValue().getNodeType();

    SchemaBuilder.FieldBuilder<Schema> fieldBuilder = assembler.name(entry.getKey());

    if (docVisitor != null) fieldBuilder = fieldBuilder.doc(docVisitor.doc(entry.getKey(), false));

    // primitive types
    switch (nodeType) {
      case BOOLEAN:
        assembler = fieldBuilder.type().optional().booleanType();
        break;

      case NUMBER:
        if (entry.getValue().isDouble()) {
          assembler = fieldBuilder.type().optional().doubleType();
        }
        else if (entry.getValue().isFloat()) {
          assembler = fieldBuilder.type().optional().floatType();
        }
        else if (entry.getValue().isShort() || entry.getValue().isInt()) {
          assembler = fieldBuilder.type().optional().intType();
        }
        else if (entry.getValue().isLong()) {
          assembler = fieldBuilder.type().optional().longType();
        }
        break;

      case BINARY:
        assembler = fieldBuilder.type().optional().bytesType();
        break;

      case STRING:
      case NULL:
      default:
        assembler = fieldBuilder.type().optional().stringType();
    }

    return assembler;
  }

  /**
   * Visitor interface to update the Avro doc fields.
   */
  @FunctionalInterface
  public interface DocVisitor {

    /**
     * Set the Avro doc field for the current encountered field or type.
     *
     * @param name the name of the field or record type
     * @param isType <code>true</code> if record type, <code>false</code> if field
     * @return a {@link String}
     */
    String doc(String name, boolean isType);

  }

}

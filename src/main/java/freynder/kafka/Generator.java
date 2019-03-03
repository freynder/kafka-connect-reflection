package freynder.kafka;

import java.util.LinkedHashMap;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.reflect.ReflectData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Generator {

  private static final String OPTIONAL = "OPTIONAL_";

  private static Logger log = LoggerFactory.getLogger(Generator.class);

  // Keep track of optional/mandatory references for a generated schema.
  // Will be used to finalize schema builder statements with optional() or not.
  private static class SchemaReference {
    private boolean optionalVersion = false;
    private boolean mandatoryVersion = false;
    private StringBuilder builderStatement;

    public SchemaReference(StringBuilder builderStatement) {
      this.builderStatement = builderStatement;
    }

    public boolean isOptionalVersion() {
      return optionalVersion;
    }

    public void setOptionalVersion(boolean optionalVersion) {
      this.optionalVersion = optionalVersion;
    }

    public boolean isMandatoryVersion() {
      return mandatoryVersion;
    }

    public void setMandatoryVersion(boolean mandatoryVersion) {
      this.mandatoryVersion = mandatoryVersion;
    }

    public StringBuilder getBuilderStatement() {
      return builderStatement;
    }
  }

  // Map to keep track of builder statements. Will be used to generate optional
  // and non-optional variants after processing the complete Avro Schema
  // Iteration order is predictable which is necessary for our schema dependencies
  private LinkedHashMap<String, SchemaReference> schemaReferences = new LinkedHashMap<>();

  public <T> String generate(Class<T> cl) {
    // Generate partial schema implementations
    from(cl);
    return provideResults();
  }

  public String provideResults() {
    final StringBuilder builder = new StringBuilder();
    schemaReferences.forEach((key, value) -> builder.append(finalize(key, value)));
    return builder.toString();
  }

  private String finalize(String name, SchemaReference schemaRef) {
    StringBuilder builder = schemaRef.getBuilderStatement();
    StringBuilder newBuilder = new StringBuilder();
    if (schemaRef.isMandatoryVersion()) {
      newBuilder.append("Schema ");
      newBuilder.append(name);
      newBuilder.append(" = ");
      newBuilder.append(builder);
      newBuilder.append(".build();\n");
    }
    if (schemaRef.isOptionalVersion()) {
      newBuilder.append("Schema ");
      newBuilder.append(OPTIONAL + name);
      newBuilder.append(" = ");
      newBuilder.append(builder);
      newBuilder.append("optional()");
      newBuilder.append(".build();\n");
    }
    return newBuilder.toString();
  }

  /**
   * Use Avro reflection to generate avro schema and get the process started
   * 
   * @param cl The class to process
   */
  public <T> void from(Class<T> cl) {
    log.debug("Processing [{}]");
    Schema avroSchema = ReflectData.get().getSchema(cl);
    log.trace("Avro schema: [{}]", avroSchema);
    fromAvroRecord(avroSchema, false);
  }

  private String fromAvroSchema(Schema avroSchema, boolean optional) {
    switch (avroSchema.getType()) {
    // RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES, INT, LONG, FLOAT,
    // DOUBLE, BOOLEAN, NULL;
    case NULL:
      throw new RuntimeException("NULL values only supported in UNION");
    case UNION:
      return fromAvroUnion(avroSchema);
    case BOOLEAN:
      return fromAvroBoolean(avroSchema, optional);
    case DOUBLE:
      return fromAvroDouble(avroSchema, optional);
    case FLOAT:
      return fromAvroFloat(avroSchema, optional);
    case LONG:
      return fromAvroLong(avroSchema, optional);
    case INT:
      return fromAvroInt(avroSchema, optional);
    case BYTES:
      return fromAvroBytes(avroSchema, optional);
    case STRING:
      return fromAvroString(avroSchema, optional);
    case FIXED:
      return fromAvroFixed(avroSchema, optional);
    case MAP:
      return fromAvroMap(avroSchema, optional);
    case ARRAY:
      return fromAvroArray(avroSchema, optional);
    case ENUM:
      return fromAvroEnum(avroSchema, optional);
    case RECORD:
      return fromAvroRecord(avroSchema, optional);
    default:
      throw new RuntimeException("Unhandled type");
    }
  }

  private String fromAvroUnion(Schema avroSchema) {
    // Polymorphism is not supported in Kafka schema specificiation so we only allow
    // union
    // to be used as a marker for optional fields, which will be reflected in the
    // generated name.
    if (!checkValidUnion(avroSchema)) {
      throw new RuntimeException(
          "Polymorphism is not allowed. Unions may only consist of a combination of null with 1 type to specify an optional field.");
    }
    if (avroSchema.getTypes().get(0).getType() != Schema.Type.NULL) {
      return fromAvroSchema(avroSchema.getTypes().get(0), true);
    } else {
      assert avroSchema.getTypes().get(1).getType() != Schema.Type.NULL;
      return fromAvroSchema(avroSchema.getTypes().get(1), true);
    }
  }

  private static boolean checkValidUnion(Schema avroSchema) {
    if (avroSchema.getTypes().size() > 2) {
      return false;
    }
    if (avroSchema.getTypes().get(0).getType() == Schema.Type.NULL
        || avroSchema.getTypes().get(1).getType() == Schema.Type.NULL) {
      return true;
    }
    return false;
  }

  private static String fromAvroBoolean(Schema avroSchema, boolean optional) {
    if (optional) {
      return "Schema.OPTIONAL_BOOLEAN_SCHEMA";
    } else {
      return "Schema.BOOLEAN_SCHEMA";
    }
  }

  private String fromAvroDouble(Schema avroSchema, boolean optional) {
    if (optional) {
      return "Schema.OPTIONAL_FLOAT64_SCHEMA";
    } else {
      return "Schema.FLOAT64_SCHEMA";
    }
  }

  private String fromAvroFloat(Schema avroSchema, boolean optional) {
    if (optional) {
      return "Schema.OPTIONAL_FLOAT32_SCHEMA";
    } else {
      return "Schema.FLOAT32_SCHEMA";
    }
  }

  private String fromAvroLong(Schema avroSchema, boolean optional) {
    if (optional) {
      return "Schema.OPTIONAL_INT64_SCHEMA";
    } else {
      return "Schema.INT64_SCHEMA";
    }
  }

  private String fromAvroInt(Schema avroSchema, boolean optional) {
    if (optional) {
      return "Schema.OPTIONAL_INT32_SCHEMA";
    } else {
      return "Schema.INT32_SCHEMA";
    }
  }

  private String fromAvroBytes(Schema avroSchema, boolean optional) {
    if (optional) {
      return "Schema.OPTIONAL_BYTES_SCHEMA";
    } else {
      return "Schema.BYTES_SCHEMA";
    }
  }

  private String fromAvroString(Schema avroSchema, boolean optional) {
    if (optional) {
      return "Schema.OPTIONAL_STRING_SCHEMA";
    } else {
      return "Schema.STRING_SCHEMA";
    }
  }

  private String fromAvroFixed(Schema avroSchema, boolean optional) {
    // no corresponding kafka connect schema for fixed, converting to more generic
    // bytes schema
    if (optional) {
      return "Schema.OPTIONAL_BYTES_SCHEMA";
    } else {
      return "Schema.BYTES_SCHEMA";
    }
  }

  private String fromAvroMap(Schema avroSchema, boolean optional) {
    // Note: could not map Schema to MapSchema since MapSchema is a private class in
    // Schema and thus not accessible. Needs further investigation but it might
    // prove difficult to determine the value type from the Schema object. A
    // possible solution would be to access the JSON representation instead of the
    // Schema object.

    // Partial implementation due to reasons stated above rather then throwing the
    // exception
    // throw new NotImplementedException("Maps are currently not supported");
    return "SchemaBuilder.map(Schema.STRING_SCHEMA, REPLACE_ME_WITH_VALUE_SCHEMA)" + (optional ? ".optional()" : "")
        + ".build()";
  }

  private String fromAvroArray(Schema avroSchema, boolean optional) {
    // Note: could not map Schema to ArraySchema since ArraySchema is a private
    // class in
    // Schema and thus not accessible. Needs further investigation but it might
    // prove difficult to determine the value type from the Schema object. A
    // possible solution would be to access the JSON representation instead of the
    // Schema object.

    // Partial implementation due to reasons stated above rather then throwing the
    // exception
    // throw new NotImplementedException("Maps are currently not supported");
    return "SchemaBuilder.array(REPLACE_ME_WITH_VALUE_SCHEMA)" + (optional ? ".optional()" : "") + ".build()";
  }

  private String fromAvroEnum(Schema avroSchema, boolean optional) {
    // Note: no matching type in Kafka Connect schema, treat as a string
    if (optional) {
      return "Schema.OPTIONAL_STRING_SCHEMA";
    } else {
      return "Schema.STRING_SCHEMA";
    }
  }

  private String fromAvroRecord(Schema avroSchema, boolean optional) {
    String name = generateSchemaName(avroSchema);
    SchemaReference schemaRef = null;
    if (!schemaReferences.containsKey(name)) {
      // Unknown type. Need to generate schema for it
      StringBuilder builderStatement = fromNewAvroRecord(avroSchema);
      schemaRef = new SchemaReference(builderStatement);
      schemaReferences.put(name, schemaRef);
    } else {
      // We already have a schema for this. Retrieve it
      schemaRef = schemaReferences.get(name);
    }
    // Update usage info
    if (optional) {
      schemaRef.setOptionalVersion(true);
      return OPTIONAL + name;
    } else {
      schemaRef.setMandatoryVersion(true);
      return name;
    }
  }

  /**
   * Generate new Kafka Connect schema builder string and register it in our
   * SchemaReference map
   */
  private StringBuilder fromNewAvroRecord(Schema avroSchema) {
    StringBuilder builder = new StringBuilder("SchemaBuilder.struct().name(\"" + avroSchema.getFullName() + "\")");
    for (Field field : avroSchema.getFields()) {
      String fieldSchemaName = fromAvroSchema(field.schema(), false);
      builder.append(".field(\"" + field.name() + "\", " + fieldSchemaName + ")");
    }
    return builder;
  }

  private String generateSchemaName(Schema avroSchema) {
    return "schema_" + (avroSchema.getFullName().replaceAll("\\.", "_"));
  }

}

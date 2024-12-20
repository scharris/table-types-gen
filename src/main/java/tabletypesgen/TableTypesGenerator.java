package tabletypesgen;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import static java.util.Collections.emptyList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import java.util.stream.Stream;

import org.checkerframework.checker.nullness.qual.Nullable;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import tabletypesgen.DatabaseMetadata.Field;
import tabletypesgen.DatabaseMetadata.RelMetadata;
import tabletypesgen.DatabaseMetadata.Enum;
import static tabletypesgen.DatabaseMetadata.RelType.table;
import static tabletypesgen.util.Args.pluckNonEmptyStringOption;
import static tabletypesgen.util.Args.pluckStringOption;
import static tabletypesgen.util.StringFuns.*;

public class TableTypesGenerator
{
  private final Map<String,FieldCustomization> fieldCustomizations;
  private final PropertyNameStyle propNameStyle;
  private final ParameterNameStyle paramNameStyle;
  private final SchemaNameStyle schemaNameStyle;
  private final TableNameStyle tableNameStyle;
  private final UserDefinedTypeNameStyle userDefinedTypeNameStyle;
  private final String schemaClassNamePrefix;
  private final String insertRecordNameSuffix;
  private final ObjectMapper objMapper;

  private static final String DEFAULT_INSERT_RECORD_NAME_SUFFIX = "_Ins";

  TableTypesGenerator
    (
      Map<String,FieldCustomization> fieldCustomizations,
      PropertyNameStyle propNameStyle,
      ParameterNameStyle paramNameStyle,
      SchemaNameStyle schemaNameStyle,
      TableNameStyle tableNameStyle,
      UserDefinedTypeNameStyle userDefinedTypeNameStyle,
      String schemaClassNamePrefix,
      String insertRecordNameSuffix,
      ObjectMapper objMapper
    )
  {
    this.fieldCustomizations = fieldCustomizations;
    this.propNameStyle = propNameStyle;
    this.paramNameStyle = paramNameStyle;
    this.schemaNameStyle = schemaNameStyle;
    this.tableNameStyle = tableNameStyle;
    this.userDefinedTypeNameStyle = userDefinedTypeNameStyle;
    this.schemaClassNamePrefix = schemaClassNamePrefix;
    this.insertRecordNameSuffix = insertRecordNameSuffix;
    this.objMapper = objMapper;
  }

  final static String USAGE_MSG = """
      Expected arguments: [options] <dbmd-file> <source-base-dir> <java-package>
        dbmd-file: Database metadata file with json contents as produced by the single row and column result from dbmd.sql.
        source-base-dir: Base directory for the output source code files (e.g. src/main/java in a maven project).
        java-package: Java package name for the produced source files.
        schema-classname-prefix: Prefix for schema class names (avoids potential clashes with Java keywords).
        [options]:
          --customization-file: File containing a json object with entries of the form:
              "<schema>.<table>.<field>": {
                  propertyType?: Nullable<string>
                  includeInType?: Nullable<boolean>
                  includeInInsertSql?: Nullable<boolean>
              }
          --propname-style: Style for generated record properties. Valid values are DB, CAMELCASE.
          --paramname-style: Style for SQL parameters in generated insert SQL which is associated with each record type via a static member.
            Valid values are: DB, CAMELCASE, QMARK, DOLLAR_NUM.
          --schemaname-style: Style for the names of Java classes representing database schemas.
            Valid values are DB, DB_INITCAP, DB_INITCAPS, CAMELCASE.
          --tablename-style: Style for Java record names representing database tables.
            Valid values are DB, DB_INITCAP, DB_INITCAPS, CAMELCASE.
          --udtname-style: Style for Java type names representing database user-defined types.
            Valid values are DB, DB_INITCAP, DB_INITCAPS, CAMELCASE.
          --ins-record-suffix: Suffix to be added to insert record types. Defaults to "_Ins".
      """;

  public static void main(String[] args)
  {
    if ( args.length == 1 && (args[0].equals("-h") || args[0].equals("--help")) )
    {
      System.out.println(USAGE_MSG);
      return;
    }

    List<String> remArgs = new ArrayList<>(Arrays.asList(args));

    @Nullable Path fieldCustsFile = pluckNonEmptyStringOption(remArgs, "--customization-file").map(Paths::get).orElse(null);
    var propStyle = PropertyNameStyle.valueOf(pluckStringOption(remArgs, "--propname-style").orElse("DB"));
    var parStyle = ParameterNameStyle.valueOf(pluckStringOption(remArgs, "--paramname-style").orElse("DB"));
    var schemaStyle = SchemaNameStyle.valueOf(pluckStringOption(remArgs, "--schemaname-style").orElse("DB_INITCAPS"));
    var tableStyle = TableNameStyle.valueOf(pluckStringOption(remArgs, "--tablename-style").orElse("DB_INITCAPS"));
    var udtStyle = UserDefinedTypeNameStyle.valueOf(pluckStringOption(remArgs, "--udtname-style").orElse("DB_INITCAPS"));
    var insRcdNameSuffix = pluckStringOption(remArgs, "--ins-record-suffix").orElse(DEFAULT_INSERT_RECORD_NAME_SUFFIX);
    String schemaPrefix = pluckStringOption(remArgs, "--schema-classname-prefix").orElse("");

    if ( remArgs.size() != 3 )
    {
      System.err.println(USAGE_MSG);
      System.exit(1);
    }

    Path dbmdFile = Paths.get(remArgs.get(0));
    Path javaBaseDir = Paths.get(remArgs.get(1));
    String javaPackage = remArgs.get(2);

    if ( !Files.isRegularFile(dbmdFile) )
      throw new RuntimeException("Database metadata file was not found: " + dbmdFile);
    if ( !Files.isDirectory(javaBaseDir) )
      throw new RuntimeException("Source base directory was not found: " + javaBaseDir);
    if ( fieldCustsFile != null && !Files.isRegularFile(fieldCustsFile) )
      throw new RuntimeException("Customization file not found: " + fieldCustsFile);

    try
    {
      ObjectMapper objMapper = new ObjectMapper();

      Map<String,FieldCustomization> fcusts = fieldCustsFile != null
        ? objMapper.readValue(fieldCustsFile.toFile(), new TypeReference<>(){})
        : Map.of();

      var generator = new TableTypesGenerator(fcusts, propStyle, parStyle, schemaStyle, tableStyle, udtStyle, schemaPrefix, insRcdNameSuffix, objMapper);

      generator.run(dbmdFile, javaBaseDir, javaPackage);

      System.exit(0);
    }
    catch(IOException e)
    {
      System.err.println("Processing failed due to error: " + e.getMessage());
      System.exit(1);
    }
  }

  public void run
    (
      Path dbmdFile,
      Path javaBaseDir,
      String javaPackage
    )
    throws IOException
  {
    DatabaseMetadata dbmd = objMapper.readValue(dbmdFile.toFile(), DatabaseMetadata.class);

    Map<String,List<RelMetadata>> relMdsBySchema =
      dbmd.relationMetadatas().stream().filter(r -> r.relationType() == table)
      .collect(groupingBy(this::schemaOrEmpty));

    Map<String,List<Enum>> enumsBySchema =
      dbmd.enums().stream().collect(groupingBy(e -> or(e.schema(),"")));

    Path outputDir = javaBaseDir.resolve(javaPackage.replace('.', '/'));
    Files.createDirectories(outputDir);

    for (var schemaRelMdsPair: relMdsBySchema.entrySet())
    {
      String schema = schemaRelMdsPair.getKey();
      String schemaClassName = schemaClassName(schema);

      List<Enum> enums = enumsBySchema.getOrDefault(schema, emptyList());

      String schemaSource = makeSchemaSource(javaPackage, schema, schemaClassName, schemaRelMdsPair.getValue(), enums);

      Files.writeString(outputDir.resolve(schemaClassName+".java"), schemaSource);
    }
  }

  private String makeSchemaSource
    (
      String javaPackage,
      String schema,
      String schemaClassName,
      List<RelMetadata> relMds,
      List<Enum> enums
    )
  {
    StringBuilder sb = new StringBuilder();
    sb.append("package ").append(javaPackage).append(";\n");
    sb.append("""

      import java.io.InputStream;
      import java.math.*;
      import java.time.*;
      import java.util.*;
      import org.checkerframework.checker.nullness.qual.Nullable;
      import com.fasterxml.jackson.databind.JsonNode;
      import com.fasterxml.jackson.databind.node.*;

      """
    );
    sb.append("public class ").append(schemaClassName).append(" {\n\n");
    sb.append("  public static final String schemaName = \"").append(schema).append("\";\n\n");
    for (var relMd: relMds)
    {
      var insFields = getIncludedFieldsForRecordUse(relMd, RecordUse.FOR_INSERT).map(Field::name).collect(toSet());
      var qryFields = getIncludedFieldsForRecordUse(relMd, RecordUse.FOR_QUERY).map(Field::name).collect(toSet());

      if (insFields.equals(qryFields))
        sb.append(indentLines(tableRecordDef(relMd, RecordUse.FOR_ANY), 2)).append("\n\n");
      else
      {
        sb.append(indentLines(tableRecordDef(relMd, RecordUse.FOR_INSERT), 2)).append("\n\n");
        sb.append(indentLines(tableRecordDef(relMd, RecordUse.FOR_QUERY), 2)).append("\n\n");
      }
    }
    for (var e: enums)
    {
      String edef = enumDef(e);
      sb.append(indentLines(edef, 2)).append("\n\n");
    }
    sb.append("\n}\n");

    return sb.toString();
  }

  private String tableRecordDef(RelMetadata relMd, RecordUse recordUse)
  {
    StringBuilder sb = new StringBuilder();

    @Nullable String schema = relMd.relationId().schema();
    String tableName = relMd.relationId().name();
    String fqTable = schema != null ? schema + "." + tableName : tableName;
    String useDescr = recordUse.name().toLowerCase().replace('_', ' ') + " use";

    sb.append("/** Table \"").append(tableName).append("\" ").append(useDescr).append(" */\n");
    sb.append("public record ").append(tableRecordName(tableName, recordUse)).append("\n");

    sb.append("(\n");
    sb.append(getIncludedFieldsForRecordUse(relMd, recordUse)
      .map(f -> "  " + propType(fqTable, f) + " " + propName(f))
      .collect(joining(",\n"))
    );
    sb.append("\n)");

    sb.append("\n{\n");
    sb.append("  public static final String qName = \"").append(fqTable).append("\";\n\n");

    if (recordUse == RecordUse.FOR_INSERT || recordUse == RecordUse.FOR_ANY)
      appendInsertSqlDef(fqTable, relMd.fields(), sb);
    if (recordUse == RecordUse.FOR_QUERY || recordUse == RecordUse.FOR_ANY)
      appendQuerySqlDef(fqTable, relMd.fields(), sb);

    sb.append("}\n");


    return sb.toString();
  }

  private void appendInsertSqlDef(String fqTable, List<Field> fields, StringBuilder sb)
  {
    List<Field> insertFields = fields.stream()
      .filter(f-> includeFieldInInsertSql(f, fqTable + "." + f.name()))
      .toList();
    String insertFieldNames = insertFields.stream().map(Field::name).collect(joining(","));

    String insertParams =
      IntStream.range(0, insertFields.size())
      .mapToObj(i -> sqlParamRef(insertFields.get(i), i))
      .collect(joining(","));

    List<String> returningFieldNames =
      fields.stream()
      .filter(f-> "ALWAYS".equals(f.identityGeneration()))
      .map(Field::name)
      .toList();
    String returningClause = returningFieldNames.isEmpty() ? "" :
      ("    returning " + String.join(",", returningFieldNames) + "\n");

    sb.append("  public static final String insertSql =\n");
    sb.append("    \"\"\"\n");
    sb.append("    insert into ").append(fqTable).append("(").append(insertFieldNames).append(")\n");
    sb.append("    values(").append(insertParams).append(")\n");
    sb.append(returningClause);
    sb.append("    \"\"\";\n");
  }

  private void appendQuerySqlDef(String fqTable, List<Field> fields, StringBuilder sb)
  {
    String selectFieldNames = fields.stream().map(Field::name).collect(joining(","));

    sb.append("  public static final String querySql =\n");
    sb.append("    \"\"\"\n");
    sb.append("    select ").append(selectFieldNames).append("\n");
    sb.append("    from ").append(fqTable).append("\n");
    sb.append("    \"\"\";\n");
  }

  private String schemaClassName(String schema)
  {
    var name = schema.isEmpty() ? "public" : schema;
    return schemaClassNamePrefix +
      switch (schemaNameStyle)
      {
        case DB_INITCAPS -> capitalizeParts(name);
        case DB_INITCAP -> capitalize(name);
        case CAMELCASE -> upperCamelCase(name);
        case DB -> name;
      };
  }

  private String tableRecordName(String tableName, RecordUse recordUse)
  {
    String baseName = switch (tableNameStyle)
    {
      case DB_INITCAPS -> capitalizeParts(tableName);
      case DB_INITCAP -> capitalize(tableName);
      case CAMELCASE -> upperCamelCase(tableName);
      case DB -> tableName;
    };

    return baseName + (recordUse == RecordUse.FOR_INSERT ? insertRecordNameSuffix : "");
  }

  private String userDefinedTypeName(@Nullable String schema, String name)
  {
    @Nullable String schemaClassName = schema != null ? schemaClassName(schema) : null;

    String typeName =
      switch (userDefinedTypeNameStyle)
      {
        case DB_INITCAPS -> capitalizeParts(name);
        case DB_INITCAP -> capitalize(name);
        case CAMELCASE -> upperCamelCase(name);
        case DB -> name;
      };

    return (schemaClassName != null) ? schemaClassName + "." + typeName : typeName;
  }

  private String propType(String fqTable, Field f)
  {
    @Nullable FieldCustomization cust = fieldCustomizations.get(fqTable + "." + f.name());
    if (cust != null && cust.propertyType != null)
      return cust.propertyType;

    return defaultJavaTypeForTableField(f);
  }

  private String propName(Field f)
  {
    return propNameStyle == PropertyNameStyle.DB ? f.name() : lowerCamelCase(f.name());
  }

  private String sqlParamRef(Field f, int fieldIx)
  {
    var bareParam =
      switch(paramNameStyle)
      {
        case DB -> ":" + f.name();
        case CAMELCASE -> ":" + lowerCamelCase(f.name());
        case QMARK -> "?";
        case DOLLAR_NUM -> "$" + (fieldIx + 1);
      };

    return f.typeUserDefined() ? bareParam+"::"+f.typeSchema()+"."+f.type() : bareParam;
  }

  private Stream<Field> getIncludedFieldsForRecordUse(RelMetadata relMd, RecordUse recordUse)
  {
    @Nullable String schema = relMd.relationId().schema();
    String tableName = relMd.relationId().name();
    String fqTable = schema != null ? schema + "." + tableName : tableName;
    return relMd.fields().stream().filter(f -> includeFieldInType(f, fqTable + "." + f.name(), recordUse));
  }

  private boolean includeFieldInType(Field f, String fqField, RecordUse recordUse)
  {
    FieldCustomization fcust = fieldCustomizations.get(fqField);

    return
      fcust != null && fcust.includeInType != null
        ? fcust.includeInType
        : recordUse == RecordUse.FOR_QUERY || !"ALWAYS".equals(f.identityGeneration());
  }

  private boolean includeFieldInInsertSql(Field f, String fqField)
  {
    @Nullable FieldCustomization fc = fieldCustomizations.get(fqField);

    return fc != null && fc.includeInInsertSql != null ? fc.includeInInsertSql
      : !"ALWAYS".equals(f.identityGeneration());
  }

  private String defaultJavaTypeForTableField(Field f)
  {
    if (f.typeUserDefined())
      return withNullability(f.nullable(), userDefinedTypeName(f.typeSchema(), f.type()));

    String lcDbFieldType = f.type().toLowerCase();

    return switch (lcDbFieldType)
    {
      case "float", "real", "double", "double precision" ->
        withNullability(f.nullable(), "double");
      case "number", "numeric", "decimal" ->
        withNullability(f.nullable(), "BigDecimal");
      case "int", "integer", "bigint", "smallint", "int8", "int4", "int2",
           "serial", "smallserial", "bigserial" ->
      {
        Integer p = f.precision();
        var primTypeName = p == null || p > 9 ? "long" : "int";
        yield withNullability(f.nullable(), primTypeName);
      }
      case "varchar", "varchar2", "text", "longvarchar", "char", "bpchar",
           "clob", "xml", "tsvector" ->
        withNullability(f.nullable(), "String");
      case "uuid" ->
        withNullability(f.nullable(), "UUID");
      case "timestamp with time zone", "timestamptz" ->
        withNullability(f.nullable(), "Instant");
      case "timestamp" ->
        withNullability(f.nullable(), "LocalDateTime");
      case "date" ->
        withNullability(f.nullable(), "LocalDate");
      case "time" ->
        withNullability(f.nullable(), "LocalTime");
      case "bit", "boolean", "bool" ->
        withNullability(f.nullable(), "boolean");
      case "bytea" ->
        withNullability(f.nullable(), "byte[]");
      case "json", "jsonb" ->
        withNullability(f.nullable(), "JsonNode");
      case "oid" ->
        withNullability(f.nullable(), "InputStream");
      default -> {
        if (lcDbFieldType.startsWith("timestamp"))
          yield withNullability(f.nullable(), "String");
        else
          throw new RuntimeException("Unsupported type for field " + f.name() + " of type " + f.type());
      }
    };
  }

  private String withNullability(@Nullable Boolean maybeNullable, String typeName)
  {
    var nullable = maybeNullable == null || maybeNullable;
    if (!nullable) return typeName;

    int typeDotIx = typeName.indexOf('.');
    if (typeDotIx != -1)
      return typeName.substring(0, typeDotIx + 1) + "@Nullable " + typeName.substring(typeDotIx + 1);
    else return "@Nullable " + toReferenceType(typeName);
  }

  private String toReferenceType(String typeName)
  {
    return switch (typeName)
    {
      case "int" -> "Integer";
      case "long" -> "Long";
      case "double" -> "Double";
      case "float" -> "Float";
      case "boolean" -> "Boolean";
      case "char" -> "Character";
      case "short" -> "Short";
      case "byte" -> "Byte";
      default -> typeName;
    };
  }

  private String enumDef(Enum e)
  {
    StringBuilder sb = new StringBuilder();
    String name = e.name();

    sb.append("/** Enum ").append(name).append(" */\n");
    sb.append("public enum ").append(userDefinedTypeName(null, name)).append("\n");

    sb.append("{\n");
    sb.append(e.labels().stream().map(l -> "  " + javaEnumLabelFromPg(l)).collect(joining(",\n")));
    sb.append("\n}\n");
    return sb.toString();
  }

  private static String javaEnumLabelFromPg(String pgEnumLabel)
  {
    return
      pgEnumLabel
      .replaceAll(",", "_comma_")
      .replaceAll("/", "_slash_")
      .replaceAll("\\.", "_dot_")
      .replaceAll("-", "_hyphen_")
      .replaceAll("\\(", "_lp_")
      .replaceAll("\\)", "_rp_")
      .replaceAll("\\[", "_lsb_")
      .replaceAll("]", "_rsb_")
      .replaceAll("\\{", "_lcb_")
      .replaceAll("}", "_rcb_")
      .replaceAll("<", "_lab_")
      .replaceAll(">", "_rab_")
      .replaceAll("\\s", "_");
  }

  private String schemaOrEmpty(RelMetadata rel)
  {
    @Nullable String schema = rel.relationId().schema();
    return (schema != null) ? schema : "";
  }

  enum PropertyNameStyle { DB, CAMELCASE }
  enum ParameterNameStyle { DB, CAMELCASE, QMARK, DOLLAR_NUM}
  enum TableNameStyle { DB, DB_INITCAP, DB_INITCAPS, CAMELCASE }
  enum UserDefinedTypeNameStyle { DB, DB_INITCAP, DB_INITCAPS, CAMELCASE }
  enum SchemaNameStyle { DB, DB_INITCAP, DB_INITCAPS, CAMELCASE }
  enum RecordUse { FOR_INSERT, FOR_QUERY, FOR_ANY }

  record FieldCustomization
  (
    @Nullable String propertyType,
    @Nullable Boolean includeInType,
    @Nullable Boolean includeInInsertSql
  ) {}

}

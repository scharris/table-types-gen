package tabletypesgen;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.IntStream;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import org.checkerframework.checker.nullness.qual.Nullable;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import static tabletypesgen.DatabaseMetadata.*;
import static tabletypesgen.util.Args.pluckNonEmptyStringOption;
import static tabletypesgen.util.Args.pluckStringOption;
import static tabletypesgen.DatabaseMetadata.RelType.table;
import static tabletypesgen.util.StringFuns.capitalize;
import static tabletypesgen.util.StringFuns.indentLines;
import static tabletypesgen.util.StringFuns.lowerCamelCase;
import static tabletypesgen.util.StringFuns.upperCamelCase;

public class TableTypesGenerator
{
  private final Map<String,FieldCustomization> fieldCustomizations;
  private final PropertyNameStyle propNameStyle;
  private final ParameterNameStyle paramNameStyle;
  private final TableNameStyle tableNameStyle;
  private final String schemaClassNamePrefix;
  private final ObjectMapper objMapper;

  TableTypesGenerator
    (
      Map<String, FieldCustomization> fieldCustomizations,
      PropertyNameStyle propNameStyle,
      ParameterNameStyle paramNameStyle,
      TableNameStyle tableNameStyle,
      String schemaClassNamePrefix,
      ObjectMapper objMapper
    )
  {
    this.fieldCustomizations = fieldCustomizations;
    this.propNameStyle = propNameStyle;
    this.paramNameStyle = paramNameStyle;
    this.tableNameStyle = tableNameStyle;
    this.schemaClassNamePrefix = schemaClassNamePrefix;
    this.objMapper = objMapper;
  }

  final static String USAGE_MSG = """
      Expected arguments: [options] <dbmd-file> <source-base-dir> <java-package>
        dbmd-file: Database metadata file with json contents as produced by the single row and column result from dbmd.sql.
        source-base-dir: Base directory for the output source code files (e.g. src/main/java in a maven project).
        java-package: Java package name for the produced source files.
        schema-classname-prefix: Prefix for schema class names (avoids potential clashes with Java keywords).
        [options]:
          --customization-file: File containing a json object with optional properties:
              propertyType?: Nullable<string>
              includeInType?: Nullable<boolean>
              includeInInsertSql?: Nullable<boolean>
          --propname-style: Style for generated record properties. Valid values are DB, CAMELCASE.
          --paramname-style: Style for SQL parameters in generated insert SQL which is associated with each record type via a static member.
            Valid values are: DB, CAMELCASE, QMARK, DOLLAR_NUM.
          --tablename-style: Style for table record names. Valid values are DB, DB_INITCAP, CAMELCASE.
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
    PropertyNameStyle propStyle = PropertyNameStyle.valueOf(pluckStringOption(remArgs, "--propname-style").orElse("DB"));
    ParameterNameStyle paramStyle = ParameterNameStyle.valueOf(pluckStringOption(remArgs, "--paramname-style").orElse("DB"));
    TableNameStyle tblNameStyle = TableNameStyle.valueOf(pluckStringOption(remArgs, "--tablename-style").orElse("DB_INITCAP"));
    String schemaClassPrefix = pluckStringOption(remArgs, "--schema-classname-prefix").orElse("");

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

      Map<String,FieldCustomization> fieldCusts = fieldCustsFile != null
        ? objMapper.readValue(fieldCustsFile.toFile(), new TypeReference<>(){})
        : Map.of();

      var generator = new TableTypesGenerator(fieldCusts, propStyle, paramStyle, tblNameStyle, schemaClassPrefix, objMapper);

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

    Path outputDir = javaBaseDir.resolve(javaPackage.replace('.', '/'));
    Files.createDirectories(outputDir);

    for (var schemaRelMdsPair: relMdsBySchema.entrySet())
    {
      String schema = schemaRelMdsPair.getKey();
      String schemaClassName = schemaClassNamePrefix + (schema.isEmpty() ? "Public" : upperCamelCase(schema));

      String schemaSource = makeSchemaSource(javaPackage, schemaClassName, schemaRelMdsPair.getValue());

      Files.writeString(outputDir.resolve(schemaClassName+".java"), schemaSource);
    }
  }

  private String makeSchemaSource
    (
      String javaPackage,
      String schemaClassName,
      List<RelMetadata> relMds
    )
  {
    StringBuilder sb = new StringBuilder();
    sb.append("package ").append(javaPackage).append(";\n");
    sb.append("""
      
      import java.util.*;
      import java.math.*;
      import java.time.*;
      import org.checkerframework.checker.nullness.qual.Nullable;
      import com.fasterxml.jackson.databind.JsonNode;
      import com.fasterxml.jackson.databind.node.*;
      
      """
    );
    sb.append("public class ").append(schemaClassName).append(" {\n\n");
    for (var relMd: relMds)
    {
      String recordDef = tableRecordDef(relMd);
      sb.append(indentLines(recordDef, 2)).append("\n\n");
    }
    sb.append("\n}\n");

    return sb.toString();
  }

  private String tableRecordDef(RelMetadata relMd)
  {
    StringBuilder sb = new StringBuilder();

    @Nullable String schema = relMd.relationId().schema();
    String tableName = relMd.relationId().name();
    String fqTable = schema != null ? schema + "." + tableName : tableName;

    sb.append("/** Table ").append(tableName).append(" */\n");
    sb.append("public record ").append(tableRecordName(tableName)).append("\n");

    sb.append("(\n");
    sb.append(
      relMd.fields().stream()
      .filter(f -> includeFieldInType(fqTable + "." + f.name()))
      .map(f -> "  " + propType(fqTable, f) + " " + propName(f))
      .collect(joining(",\n"))
    );
    sb.append("\n)\n");

    sb.append("{\n");

    List<Field> insertFields = relMd.fields().stream()
      .filter(f-> includeFieldInInsertSql(fqTable + "." + f.name()))
      .toList();
    String insertFieldNames = insertFields.stream().map(Field::name).collect(joining(","));
    String insertParams =
      IntStream.range(0, insertFields.size())
      .mapToObj(i -> paramName(insertFields.get(i), i))
      .collect(joining(","));

    sb.append("  public static final String insertSql =\n");
    sb.append("    \"\"\"\n");
    sb.append("    insert into ").append(fqTable).append("(").append(insertFieldNames).append(")\n");
    sb.append("    values(").append(insertParams).append(")\n");
    sb.append("    \"\"\";\n");

    sb.append("}\n");

    return sb.toString();
  }

  private String tableRecordName(String tableName)
  {
    return switch (tableNameStyle)
    {
      case DB_INITCAP -> capitalize(tableName);
      case CAMELCASE -> upperCamelCase(tableName);
      case DB -> tableName;
    };
  }

  private String propType(String fqTable, Field f)
  {
    String fqField = fqTable + "." + f.name();
    @Nullable FieldCustomization cust = fieldCustomizations.get(fqField);
    return cust != null && cust.propertyType != null ? cust.propertyType : defaultJavaTypeForTableField(f);
  }

  private String propName(Field f)
  {
    return propNameStyle == PropertyNameStyle.DB ? f.name() : lowerCamelCase(f.name());
  }

  private String paramName(Field f, int fieldIx)
  {
    return switch(paramNameStyle)
    {
      case DB -> ":" + f.name();
      case CAMELCASE -> ":" + lowerCamelCase(f.name());
      case QMARK -> "?";
      case DOLLAR_NUM -> "$" + (fieldIx + 1);
    };
  }

  private boolean includeFieldInType(String fqField)
  {
    FieldCustomization fc = fieldCustomizations.get(fqField);
    return fc != null && fc.includeInType != null ? fc.includeInType : true;
  }

  private boolean includeFieldInInsertSql(String fqField)
  {
    @Nullable FieldCustomization fc = fieldCustomizations.get(fqField);
    return fc != null && fc.includeInInsertSql != null ? fc.includeInInsertSql : true;
  }

  private String defaultJavaTypeForTableField(Field f)
  {
    String lcDbFieldType = f.databaseType().toLowerCase();

    return switch (lcDbFieldType)
    {
      case "float", "real", "double", "double precision" ->
        withNullability(f.nullable(), "double");
      case "number", "numeric", "decimal",
           "int", "integer", "bigint", "smallint", "int8", "int4", "int2",
           "serial", "smallserial", "bigserial" ->
        nonFloatingNumericPropertyType(f);
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
      default -> {
        if (lcDbFieldType.startsWith("timestamp"))
          yield withNullability(f.nullable(), "String");
        else
          throw new RuntimeException("Unsupported type for field " + f.name() + " of type " + f.databaseType());
      }
    };
  }

  private String nonFloatingNumericPropertyType(Field f)
  {
    if (f.fractionalDigits() == null || f.fractionalDigits() > 0)
      return withNullability(f.nullable(), "BigDecimal");
    else // no fractional part
    {
      var primTypeName = f.precision() == null || f.precision() > 9 ? "long" : "int";
      return withNullability(f.nullable(), primTypeName);
    }
  }

  private String withNullability(@Nullable Boolean maybeNullable, String typeName)
  {
    var nullable = maybeNullable == null || maybeNullable;
    return nullable ? "@Nullable " + toReferenceType(typeName) : typeName;
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

  private String schemaOrEmpty(RelMetadata rel)
  {
    @Nullable String schema = rel.relationId().schema();
    return (schema != null) ? schema : "";
  }

  enum PropertyNameStyle { DB, CAMELCASE }
  enum ParameterNameStyle { DB, CAMELCASE, QMARK, DOLLAR_NUM}
  enum TableNameStyle { DB, DB_INITCAP, CAMELCASE }

  record FieldCustomization
  (
    @Nullable String propertyType,
    @Nullable Boolean includeInType,
    @Nullable Boolean includeInInsertSql
  ) {}

}

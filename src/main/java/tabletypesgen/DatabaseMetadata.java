package tabletypesgen;

import java.util.List;

import org.checkerframework.checker.nullness.qual.Nullable;

public record DatabaseMetadata
(
  String dbmsName,
  String dbmsVersion,
  @Nullable Integer majorVersion,
  @Nullable Integer minorVersion,
  CaseSensitivity caseSensitivity,
  List<RelMetadata> relationMetadatas,
  List<ForeignKey> foreignKeys
)
{
  enum CaseSensitivity {
    INSENSITIVE_STORED_LOWER,
    INSENSITIVE_STORED_UPPER,
    INSENSITIVE_STORED_MIXED,
    SENSITIVE
  }

  enum RelType {
    table,
    view,
    unknown
  }

  record RelId
  (
    @Nullable String schema,
    String name
  ) {}

  record Field
  (
    String name,
    String databaseType,
    int jdbcTypeCode,
    @Nullable Boolean nullable,
    @Nullable Integer primaryKeyPartNumber,
    @Nullable Integer length,
    @Nullable Integer precision,
    @Nullable Integer precisionRadix,
    @Nullable Integer fractionalDigits,
    String comment
  ) {}

  record RelMetadata
  (
    RelId relationId,
    RelType relationType,
    List<Field> fields,
    @Nullable String comment
  ) {}


  record ForeignKeyComponent
  (
    String foreignKeyFieldName,
    String primaryKeyFieldName
  ) {}

  record ForeignKey
  (
    @Nullable String constraintName,
    RelId foreignKeyRelationId,
    RelId primaryKeyRelationId,
    List<ForeignKeyComponent> foreignKeyComponents
  ) {}
}

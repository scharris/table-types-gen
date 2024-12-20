with
ignoreSchemasQuery as (
  select unnest(array['pg_catalog', 'information_schema']) schema_name
),
relationMetadatasQuery as (
  select
    coalesce(json_agg(json_build_object(
      'relationId', json_build_object('schema', r.schemaname, 'name', r.name),
      'relationType', r.type,
      'fields', (
        select
          coalesce(json_agg(json_build_object(
            'name', col.column_name,
            'type', col.udt_name,
            'typeSchema', col.udt_schema,
            'typeUserDefined', col.data_type = 'USER-DEFINED',
            'identityGeneration', col.identity_generation,
            'isIdentity', col.is_identity = 'YES',
            'nullable', case col.is_nullable when 'NO' then false when 'YES' then true end,
            'primaryKeyPartNumber', (
              select kcu.ordinal_position
              from information_schema.key_column_usage kcu
              where
                kcu.table_schema = col.table_schema and
                kcu.table_name = col.table_name and
                kcu.column_name = col.column_name and
                kcu.constraint_name = (
                  select constraint_name
                  from information_schema.table_constraints tc
                  where tc.constraint_type = 'PRIMARY KEY' and
                    tc.table_schema = r.schemaname and
                    tc.table_name = r.name
                )
            ),
            'length', col.character_maximum_length,
            'precision', col.numeric_precision,
            'precisionRadix', col.numeric_precision_radix,
            'fractionalDigits', col.numeric_scale
          ) order by col.ordinal_position), '[]'::json)
        from information_schema.columns col
        where col.table_schema = r.schemaname and col.table_name = r.name
      ) -- fields property
    )), '[]'::json) json
  from (
    select t.schemaname, t.tablename name, 'table' type
    from pg_tables t
    union all
    select v.schemaname, v.viewname name, 'view' type
    from pg_views v
  ) r
  where r.schemaname not in (select * from ignoreSchemasQuery)
    -- and r.schemaname || '.' || r.name ~ :relIncludePat
    -- and r.schemaname || '.' || r.name !~ :relExcludePat
),
foreignKeysQuery as (
  select coalesce(json_agg(fk.obj), '[]'::json) json
  from (
    select
      json_build_object(
        'constraintName', child_tc.constraint_name,
        'foreignKeyRelationId',
          json_build_object(
            'schema', child_tc.table_schema,
            'name', child_tc.table_name
          ),
        'primaryKeyRelationId',
          json_build_object(
            'schema', parent_tc.table_schema,
            'name', parent_tc.table_name
          ),
        'foreignKeyComponents',
          json_agg(
            json_build_object(
              'foreignKeyFieldName', child_fk_comp.column_name,
              'primaryKeyFieldName', parent_pk_comp.column_name
            )
            order by child_fk_comp.ordinal_position
          )
      ) obj
    from information_schema.table_constraints child_tc
    -- To-one join for parent's pk constraint schema and name.
    join information_schema.referential_constraints child_rc
      on  child_tc.constraint_schema = child_rc.constraint_schema
      and child_tc.constraint_name = child_rc.constraint_name
    -- To-one join for parent's pk constraint information.
    join information_schema.table_constraints parent_tc
      on  child_rc.unique_constraint_schema = parent_tc.constraint_schema
      and child_rc.unique_constraint_name = parent_tc.constraint_name
    -- To-many join for the field components of the foreign key in the child table.
    join information_schema.key_column_usage child_fk_comp
      on  child_tc.constraint_schema = child_fk_comp.constraint_schema
      and child_tc.constraint_name = child_fk_comp.constraint_name
    -- To-one join for the parent table fk field component which is matched to that of the child.
    join information_schema.key_column_usage parent_pk_comp
      on  child_rc.unique_constraint_schema = parent_pk_comp.constraint_schema
      and child_rc.unique_constraint_name = parent_pk_comp.constraint_name
      and child_fk_comp.position_in_unique_constraint = parent_pk_comp.ordinal_position
    where child_tc.constraint_type = 'FOREIGN KEY'
      and child_fk_comp.table_schema not in (select * from ignoreSchemasQuery)
      -- and child_tc.table_schema || '.' || child_tc.table_name ~ :relIncludePat
      -- and child_tc.table_schema || '.' || child_tc.table_name !~ :relExcludePat
      -- and parent_tc.table_schema || '.' || parent_tc.table_name ~ :relIncludePat
      -- and parent_tc.table_schema || '.' || parent_tc.table_name !~ :relExcludePat
    group by
      child_tc.table_schema,
      child_tc.table_name,
      parent_tc.table_name,
      parent_tc.table_schema,
      child_tc.constraint_name
  ) fk
),
enumsQuery as (
  select coalesce(json_agg(enum.json), '[]'::json) json
  from (
    select
      json_build_object(
        'schema', n.nspname,
        'name', t.typname,
        'labels', json_agg(e.enumlabel order by e.ctid)
      ) json
    from pg_type t
    join pg_enum e on t.oid = e.enumtypid
    join pg_catalog.pg_namespace n ON n.oid = t.typnamespace
    group by n.nspname, t.typname
  ) enum
)
-- main query
select jsonb_pretty(jsonb_build_object(
  'dbmsName', 'PostgreSQL',
  'dbmsVersion', split_part(version(), ' ', 2),
  'caseSensitivity', 'INSENSITIVE_STORED_LOWER',
  'relationMetadatas', (select json from relationMetadatasQuery),
  'foreignKeys', (select json from foreignKeysQuery),
  'enums', (select json from enumsQuery)
)) json
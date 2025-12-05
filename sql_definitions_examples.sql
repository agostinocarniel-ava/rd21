-- View definition (exact CREATE VIEW text)
-- Replace schema and view name
SELECT OBJECT_DEFINITION(OBJECT_ID(N'[schema].[view_name]')) AS [ViewDefinition];
GO

-- Alternative: via sys.sql_modules
SELECT sm.definition
FROM sys.sql_modules AS sm
JOIN sys.objects     AS o ON sm.object_id = o.object_id
WHERE o.type = 'V' AND o.[name] = 'view_name' AND SCHEMA_NAME(o.schema_id) = 'schema';
GO

-- Stored procedure definition (exact CREATE text)
SELECT OBJECT_DEFINITION(OBJECT_ID(N'[schema].[proc_name]')) AS [ProcDefinition];
GO

-- Function definition (exact CREATE text)
SELECT OBJECT_DEFINITION(OBJECT_ID(N'[schema].[function_name]')) AS [FunctionDefinition];
GO

-- Table definition (best-effort CREATE TABLE from metadata)
DECLARE @schema sysname = N'schema', @table sysname = N'table_name';

WITH cols AS (
  SELECT
    c.column_id,
    QUOTENAME(c.name) AS col_name,
    t.name AS type_name,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.is_identity,
    ic.seed_value,
    ic.increment_value,
    c.collation_name
  FROM sys.columns c
  JOIN sys.types   t  ON c.user_type_id = t.user_type_id
  LEFT JOIN sys.identity_columns ic ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
  WHERE c.[object_id] = OBJECT_ID(QUOTENAME(@schema) + N'.' + QUOTENAME(@table))
),
 pk AS (
  SELECT k.name AS pk_name,
         STRING_AGG(QUOTENAME(c.name), N',') WITHIN GROUP (ORDER BY c.column_id) AS pk_cols
  FROM sys.key_constraints k
  JOIN sys.index_columns ic ON ic.[object_id] = k.[parent_object_id] AND ic.index_id = k.unique_index_id
  JOIN sys.columns c        ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
  WHERE k.[type] = 'PK' AND k.[parent_object_id] = OBJECT_ID(QUOTENAME(@schema) + N'.' + QUOTENAME(@table))
  GROUP BY k.name
)
SELECT
  'CREATE TABLE ' + QUOTENAME(@schema) + '.' + QUOTENAME(@table) + CHAR(10) +
  '(' + CHAR(10) +
  STRING_AGG(
    '  ' + col_name + ' ' +
    CASE
      WHEN type_name IN ('varchar','char','varbinary','binary','nvarchar','nchar')
        THEN type_name + '(' + CASE WHEN max_length = -1 THEN 'MAX'
                                    WHEN type_name LIKE 'n%' THEN CAST(max_length/2 AS varchar(10))
                                    ELSE CAST(max_length AS varchar(10)) END + ')'
      WHEN type_name IN ('decimal','numeric')
        THEN type_name + '(' + CAST(precision AS varchar(10)) + ',' + CAST(scale AS varchar(10)) + ')'
      ELSE type_name
    END +
    CASE WHEN collation_name IS NOT NULL AND type_name LIKE '%char%' AND type_name NOT LIKE 'n%'
         THEN ' COLLATE ' + collation_name ELSE '' END +
    CASE WHEN is_identity = 1 THEN ' IDENTITY(' + CAST(seed_value AS varchar(20)) + ',' + CAST(increment_value AS varchar(20)) + ')' ELSE '' END +
    CASE WHEN is_nullable = 0 THEN ' NOT NULL' ELSE ' NULL' END
  , ',' + CHAR(10)) WITHIN GROUP (ORDER BY column_id) +
  CASE WHEN EXISTS (SELECT 1 FROM pk) THEN ',' + CHAR(10) +
       '  CONSTRAINT ' + (SELECT QUOTENAME(pk_name) FROM pk) +
       ' PRIMARY KEY (' + (SELECT pk_cols FROM pk) + ')' ELSE '' END + CHAR(10) +
  ');' AS [CreateTableScript]
FROM cols;
GO

-- Indexes on a table (supplement CREATE TABLE)
SELECT
  i.name AS index_name,
  i.type_desc,
  'CREATE ' +
  CASE WHEN i.is_unique = 1 THEN 'UNIQUE ' ELSE '' END +
  CASE WHEN i.type_desc = 'CLUSTERED' THEN 'CLUSTERED ' ELSE 'NONCLUSTERED ' END +
  'INDEX ' + QUOTENAME(i.name) + ' ON ' +
  QUOTENAME(SCHEMA_NAME(t.schema_id)) + '.' + QUOTENAME(t.name) + '(' +
  STRING_AGG(QUOTENAME(c.name), ',') WITHIN GROUP (ORDER BY ic.key_ordinal) + ')' +
  CASE WHEN i.has_filter = 1 THEN ' WHERE ' + i.filter_definition ELSE '' END AS create_index
FROM sys.indexes i
JOIN sys.tables t         ON t.object_id = i.object_id
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
JOIN sys.columns c        ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE t.schema_id = SCHEMA_ID('schema') AND t.name = 'table_name' AND i.is_primary_key = 0
GROUP BY i.name, i.type_desc, i.is_unique, i.has_filter, i.filter_definition, t.schema_id, t.name;
GO

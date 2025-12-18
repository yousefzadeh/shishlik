{# 
DOC START
  - name: vwTableColumnMetadata
    description: |
        List of Tables, Columns, and Column Metadata
DOC END
#}


SELECT 
    tbl.name Table_Name,
    c.name Column_Name,
    t.Name ColumnDataType_Name,
    c.max_length Column_MaxLen,
    c.precision Column_Precision,
    c.scale Column_Scale,
    c.is_nullable Column_IsNullable,
    ISNULL(i.is_primary_key, 0) Column_IsPrimaryKey,
    c.is_computed Column_IsComputed,
    cc.definition Column_ComputedDefinition
FROM {{ ref("DB_Columns") }} c
INNER JOIN {{ ref("DB_Tables") }} tbl ON c.object_id = tbl.object_id 
INNER JOIN {{ ref("DB_Types") }} t ON c.user_type_id = t.user_type_id
LEFT OUTER JOIN {{ ref("DB_IndexColumns") }} ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
LEFT OUTER JOIN {{ ref("DB_Indexes") }} i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
LEFT OUTER JOIN {{ ref("DB_ComputedColumns") }} cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id

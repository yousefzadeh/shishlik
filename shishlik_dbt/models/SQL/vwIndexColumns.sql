with final as (
select 
    tb.name Table_Name,
    ix.name Index_Name,
    ix.index_id,
    ix.type_desc,
    ix.is_unique,
    ix.is_primary_key,
    ix.is_unique_constraint,
    ix.has_filter,
    ix.filter_definition,
    col.name Column_Name,
    ixc.is_descending_key,
    ixc.is_included_column
from {{ ref("DB_Tables") }} tb
join {{ ref("DB_Indexes") }} ix
  on tb.object_id = ix.object_id
join {{ ref("DB_IndexColumns") }} ixc 
  on ix.object_id = ixc.object_id
  and ix.index_id = ixc.index_id 
join {{ ref("DB_Columns") }} col
  on ixc.object_id = col.object_id
  and ixc.column_id = col.column_id
)
select * 
from final

{{-
    config(
        {
            "materialized": "incremental",
            "tags": ["nightly"],
            "as_columnstore": false,
            "pre-hook": "{{ incremental_drop_all_indexes_on_table() }}",
            "post-hook": [
                "{{ incremental_create_clustered_index(columns = ['TenantId']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Record_LastModificationTime']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['UserName']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['UserEmail']) }}",
            ],
        }
    )
-}}
select *
from {{ ref("vwUserAssigned") }}
{% if is_incremental() -%}
     -- Incremental run
    where Record_LastModificationTime > (select max(Record_LastModificationTime) from {{ this }})
--{% else -%}
    -- -- Full run
    --where Record_LastModificationTime < '2024-11-12 00:00:00.000'  {# fake full run so incremental will have rows to add #}
{%- endif -%}
 
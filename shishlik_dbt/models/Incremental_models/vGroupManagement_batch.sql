{{-
    config(
        {
            "materialized": "incremental",
            "tags": ["nightly"],
            "as_columnstore": false,
            "unique_key": "Id",
            "pre-hook": "{{ incremental_drop_all_indexes_on_table() }}",
            "post-hook": [
                "{{ incremental_create_clustered_index(columns = ['EntityId']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Date_Time']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Tenant_Id']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Actioned_By']) }}",
            ],
        }
    )
-}}
select *
from {{ ref("vGroupManagement") }}
{% if is_incremental() -%}
     -- Incremental run
    where Date_Time > (select max(Date_Time) from {{ this }})
--{% else -%}
    -- -- Full run
    --where Date_Time < '2024-11-12 00:00:00.000'  {# fake full run so incremental will have rows to add #}
{%- endif -%}
 
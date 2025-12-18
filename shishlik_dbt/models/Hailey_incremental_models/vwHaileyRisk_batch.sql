{{
    config(
        {
            "materialized": "incremental",
            "as_columnstore": false,
            "unique_key": "union_id",
            "pre-hook": "{{ incremental_drop_all_indexes_on_table() }}",
            "post-hook": [
                "{{ incremental_create_clustered_index(columns = ['union_id']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['tenant_id']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['tenant_name']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['record_type']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['record_id']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['last_updatetime']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['status']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['text_hash']) }}",
            ],
        }
    )
}}

select
    *

from {{ ref('vwHaileyRisk') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  -- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
where last_updatetime >= (select coalesce(max(last_updatetime),'1900-01-01') from {{ this }} ) 

 {% endif %}
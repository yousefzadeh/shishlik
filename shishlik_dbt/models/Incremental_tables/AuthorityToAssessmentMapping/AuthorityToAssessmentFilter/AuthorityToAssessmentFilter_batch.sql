{{-
    config(
        {
            "materialized": "incremental",
            "tags": ["nightly"],
            "as_columnstore": false,
            "unique_key": "Filter_PK",
            "pre-hook": "{{ incremental_drop_all_indexes_on_table() }}",
            "post-hook": [
                "{{ incremental_create_clustered_index(columns = ['Filter_PK']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Authority_Name'], includes = ['Filter_PK']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Template_Name'], includes = ['Filter_PK']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Assessment_Name'], includes = ['Filter_PK']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['AuthorityProvision_Name'], includes = ['Filter_PK']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['AuthorityCustom_FieldName'], includes = ['Filter_PK']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Assessment_TenantId'], includes = ['Filter_PK']) }}",
            ],
        }
    )
-}}
select *
from
    {{ ref("vwAuthorityToAssessmentFilter_source") }}
    {% if is_incremental() -%}
    -- -- Incremental run
    -- where Filter_UpdateTime > ( select max(Filter_UpdateTime) from {{ this }} )
    {% else -%}
    -- -- Full run
    -- where Filter_UpdateTime < '2022-07-01 00:00:00.000'  {# fake full run so incremental will have rows to add #}
    {%- endif -%}
    -- Full load all the time
    

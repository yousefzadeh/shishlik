{{-
    config(
        {
            "materialized": "incremental",
            "tags": ["nightly"],
            "as_columnstore": false,
            "unique_key": "QuestionOption_PK",
            "pre-hook": "{{ incremental_drop_all_indexes_on_table() }}",
            "post-hook": [
                "{{ incremental_create_clustered_index(columns = ['Question_Id']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Question_TenantId']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['QuestionOption_Value']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Question_UpdateTime']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Question_AssessmentDomainId']) }}",
            ],
        }
    )
-}}
select top 100000 *
from {{ ref("vwQuestionOption_source") }}
{% if is_incremental() -%}
    -- -- Incremental run
    where Question_UpdateTime > (select max(Question_UpdateTime) from {{ this }})
    {% else -%}
    -- -- Full run
    -- where Question_UpdateTime < '2022-07-01 00:00:00.000'  {# fake full run so incremental will have rows to add #}
    {%- endif -%}

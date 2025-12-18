{{-
    config(
        {
            "materialized": "incremental",
            "tags": ["nightly"],
            "as_columnstore": false,
            "unique_key": "AuthorityAssessmentQuestionAnswer_PK",
            "pre-hook": "{{ incremental_drop_all_indexes_on_table() }}",
            "post-hook": [
                "{{ incremental_create_clustered_index(columns = ['Assessment_TenantId']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Assessment_Id']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Template_Name']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['Answer_Id']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['AnswerResponse_Value']) }}",
                "{{ incremental_create_nonclustered_index(columns = ['AssessmentQuestionAnswer_UpdateTime']) }}",
            ],
        }
    )
-}}
select *
from {{ ref("vwAuthorityAssessmentQuestionAnswer_source") }}
{% if is_incremental() -%}
    -- -- Incremental run
    where AssessmentQuestionAnswer_UpdateTime > (select max(AssessmentQuestionAnswer_UpdateTime) from {{ this }})
{% else -%}
    -- -- Full run
    where AssessmentQuestionAnswer_UpdateTime < '2022-07-01 00:00:00.000'  {# fake full run so incremental will have rows to add #}
{%- endif -%}

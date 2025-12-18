{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [ConditionalQuestionRefId],
            cast([ConditionalValues] as nvarchar(4000)) ConditionalValues,
            [TenantId],
            [QuestionId]
        from {{ source("assessment_models", "ConditionalQuestion") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ConditionalQuestion") }},
    {{ col_rename("ConditionalQuestionRefId", "ConditionalQuestion") }},
    {{ col_rename("ConditionalValues", "ConditionalQuestion") }},
    {{ col_rename("TenantId", "ConditionalQuestion") }},

    {{ col_rename("QuestionId", "ConditionalQuestion") }}
from base

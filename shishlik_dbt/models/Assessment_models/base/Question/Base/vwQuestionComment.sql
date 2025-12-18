{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Message] as nvarchar(4000)) Message,
            [QuestionId],
            [UserId],
            [RootQuestionId]
        from {{ source("assessment_models", "QuestionComment") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "QuestionComment") }},
    {{ col_rename("TenantId", "QuestionComment") }},
    {{ col_rename("Message", "QuestionComment") }},
    {{ col_rename("QuestionId", "QuestionComment") }},

    {{ col_rename("UserId", "QuestionComment") }},
    {{ col_rename("RootQuestionId", "QuestionComment") }}
from base

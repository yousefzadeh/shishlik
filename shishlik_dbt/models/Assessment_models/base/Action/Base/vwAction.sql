{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Task] as nvarchar(4000)) Task,
            [DueDate],
            [QuestionId],
            [UserId],
            [TenantId],
            [StatusId]
        from {{ source("assessment_models", "Action") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Action") }},
    {{ col_rename("Task", "Action") }},
    {{ col_rename("DueDate", "Action") }},
    {{ col_rename("QuestionId", "Action") }},

    {{ col_rename("UserId", "Action") }},
    {{ col_rename("TenantId", "Action") }},
    {{ col_rename("StatusId", "Action") }}
from base

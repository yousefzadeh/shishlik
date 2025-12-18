{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([CommentText] as nvarchar(4000)) CommentText,
            [ProjectTaskId],
            [TenantId],
            [UserId]
        from {{ source("project_models", "ProjectTaskComment") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ProjectTaskComment") }},
    {{ col_rename("CommentText", "ProjectTaskComment") }},
    {{ col_rename("ProjectTaskId", "ProjectTaskComment") }},
    {{ col_rename("TenantId", "ProjectTaskComment") }},

    {{ col_rename("UserId", "ProjectTaskComment") }}
from base

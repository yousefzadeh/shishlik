{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([FileName] as nvarchar(4000)) FileName,
            cast([DisplayName] as nvarchar(4000)) DisplayName,
            [ProjectTaskId],
            [TenantId]
        from {{ source("project_models", "ProjectTaskDocument") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ProjectTaskDocument") }},
    {{ col_rename("FileName", "ProjectTaskDocument") }},
    {{ col_rename("DisplayName", "ProjectTaskDocument") }},
    {{ col_rename("ProjectTaskId", "ProjectTaskDocument") }},

    {{ col_rename("TenantId", "ProjectTaskDocument") }}
from base

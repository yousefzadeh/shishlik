{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([FileName] as nvarchar(4000)) FileName,
            cast([Fileurl] as nvarchar(4000)) Fileurl,
            [LastUpload],
            [StatementResponseId] ResponsibilityTaskId,
            [UserId],
            cast([DisplayFileName] as nvarchar(4000)) DisplayFileName
        from {{ source("statement_models", "StatementResponseDocument") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ResponsibilityTaskDocument") }},
    {{ col_rename("TenantId", "ResponsibilityTaskDocument") }},
    {{ col_rename("FileName", "ResponsibilityTaskDocument") }},
    {{ col_rename("Fileurl", "ResponsibilityTaskDocument") }},

    {{ col_rename("LastUpload", "ResponsibilityTaskDocument") }},
    {{ col_rename("ResponsibilityTaskId", "ResponsibilityTaskDocument") }},
    {{ col_rename("UserId", "ResponsibilityTaskDocument") }},
    {{ col_rename("DisplayFileName", "ResponsibilityTaskDocument") }}
from base

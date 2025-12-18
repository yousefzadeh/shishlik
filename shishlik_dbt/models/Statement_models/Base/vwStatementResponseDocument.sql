{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([FileName] as nvarchar(4000)) FileName,
            cast([Fileurl] as nvarchar(4000)) Fileurl,
            [LastUpload],
            [StatementResponseId],
            [UserId],
            cast([DisplayFileName] as nvarchar(4000)) DisplayFileName
        from {{ source("statement_models", "StatementResponseDocument") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "StatementResponseDocument") }},
    {{ col_rename("TenantId", "StatementResponseDocument") }},
    {{ col_rename("FileName", "StatementResponseDocument") }},
    {{ col_rename("Fileurl", "StatementResponseDocument") }},

    {{ col_rename("LastUpload", "StatementResponseDocument") }},
    {{ col_rename("StatementResponseId", "StatementResponseDocument") }},
    {{ col_rename("UserId", "StatementResponseDocument") }},
    {{ col_rename("DisplayFileName", "StatementResponseDocument") }}
from base

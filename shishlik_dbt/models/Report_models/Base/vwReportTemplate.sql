{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [EntityType],
            cast([Title] as nvarchar(4000)) Title,
            cast([FileName] as nvarchar(4000)) FileName,
            cast([DisplayFileName] as nvarchar(4000)) DisplayFileName,
            cast([FileUrl] as nvarchar(4000)) FileUrl,
            [EntityId],
            [EntitySubType]
        from {{ source("Report_models", "ReportTemplate") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ReportTemplate") }},
    {{ col_rename("TenantId", "ReportTemplate") }},
    {{ col_rename("EntityType", "ReportTemplate") }},
    {{ col_rename("Title", "ReportTemplate") }},

    {{ col_rename("FileName", "ReportTemplate") }},
    {{ col_rename("DisplayFileName", "ReportTemplate") }},
    {{ col_rename("FileUrl", "ReportTemplate") }},
    {{ col_rename("EntityId", "ReportTemplate") }},

    {{ col_rename("EntitySubType", "ReportTemplate") }}
from base

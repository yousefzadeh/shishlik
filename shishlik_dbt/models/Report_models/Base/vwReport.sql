{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            cast([FileName] as nvarchar(4000)) FileName,
            cast([DisplayFileName] as nvarchar(4000)) DisplayFileName,
            cast([ContainerName] as nvarchar(4000)) ContainerName,
            [FileSizeInKB],
            [TenantId],
            [EntityId],
            [EntityType],
            [EntitySubType],
            [ReportTemplateId],
            [Status]
        from {{ source("Report_models", "Report") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Report") }},
    {{ col_rename("FileName", "Report") }},
    {{ col_rename("DisplayFileName", "Report") }},
    {{ col_rename("ContainerName", "Report") }},

    {{ col_rename("FileSizeInKB", "Report") }},
    {{ col_rename("TenantId", "Report") }},
    {{ col_rename("EntityId", "Report") }},
    {{ col_rename("EntityType", "Report") }},

    {{ col_rename("EntitySubType", "Report") }},
    {{ col_rename("ReportTemplateId", "Report") }},
    {{ col_rename("Status", "Report") }}
from base

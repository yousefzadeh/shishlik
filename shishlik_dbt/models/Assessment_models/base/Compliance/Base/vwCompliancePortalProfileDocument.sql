{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [CompliancePortalProfileId],
            cast([DocumentFileName] as nvarchar(4000)) DocumentFileName,
            cast([DisplayFileName] as nvarchar(4000)) DisplayFileName,
            cast([DocumentUrl] as nvarchar(4000)) DocumentUrl,
            [FileSizeInKB]
        from {{ source("assessment_models", "CompliancePortalProfileDocument") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "CompliancePortalProfileDocument") }},
    {{ col_rename("TenantId", "CompliancePortalProfileDocument") }},
    {{ col_rename("CompliancePortalProfileId", "CompliancePortalProfileDocument") }},
    {{ col_rename("DocumentFileName", "CompliancePortalProfileDocument") }},

    {{ col_rename("DisplayFileName", "CompliancePortalProfileDocument") }},
    {{ col_rename("DocumentUrl", "CompliancePortalProfileDocument") }},
    {{ col_rename("FileSizeInKB", "CompliancePortalProfileDocument") }}
from base

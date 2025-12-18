{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            CompliancePortalProfileId,
            UserId,
            OrganizationUnitId
        from {{ source("assessment_models", "CompliancePortalProfileOwner") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "CompliancePortalProfileOwner") }},
    {{ col_rename("CreationTime", "CompliancePortalProfileOwner") }},
    {{ col_rename("LastModificationTime", "CompliancePortalProfileOwner") }},
    {{ col_rename("TenantId", "CompliancePortalProfileOwner") }},
    {{ col_rename("CompliancePortalProfileId", "CompliancePortalProfileOwner") }},
    {{ col_rename("UserId", "CompliancePortalProfileOwner") }},

    {{ col_rename("OrganizationUnitId", "CompliancePortalProfileOwner") }}
from base

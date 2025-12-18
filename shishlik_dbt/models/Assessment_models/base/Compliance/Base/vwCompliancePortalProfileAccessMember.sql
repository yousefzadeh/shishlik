{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            CompliancePortalProfileId,
            UserId,
            OrganizationUnitId
        from {{ source("assessment_models", "CompliancePortalProfileAccessMember") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "CompliancePortalProfileAccessMember") }},
    {{ col_rename("CreationTime", "CompliancePortalProfileAccessMember") }},
    {{ col_rename("LastModificationTime", "CompliancePortalProfileAccessMember") }},
    {{ col_rename("TenantId", "CompliancePortalProfileAccessMember") }},
    {{ col_rename("CompliancePortalProfileId", "CompliancePortalProfileAccessMember") }},
    {{ col_rename("UserId", "CompliancePortalProfileAccessMember") }},

    {{ col_rename("OrganizationUnitId", "CompliancePortalProfileAccessMember") }}
from base

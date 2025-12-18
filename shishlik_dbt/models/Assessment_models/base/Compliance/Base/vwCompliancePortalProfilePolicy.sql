{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [CompliancePortalProfileId], [PolicyId]
        from {{ source("assessment_models", "CompliancePortalProfilePolicy") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "CompliancePortalProfilePolicy") }},
    {{ col_rename("TenantId", "CompliancePortalProfilePolicy") }},
    {{ col_rename("CompliancePortalProfileId", "CompliancePortalProfilePolicy") }},
    {{ col_rename("PolicyId", "CompliancePortalProfilePolicy") }}
from base

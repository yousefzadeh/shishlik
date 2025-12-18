{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [CompliancePortalProfileId], [AssessmentId]
        from {{ source("assessment_models", "CompliancePortalProfileAssessment") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "CompliancePortalProfileAssessment") }},
    {{ col_rename("TenantId", "CompliancePortalProfileAssessment") }},
    {{ col_rename("CompliancePortalProfileId", "CompliancePortalProfileAssessment") }},
    {{ col_rename("AssessmentId", "CompliancePortalProfileAssessment") }}
from base

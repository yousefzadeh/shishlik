{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [AssessmentDomainId], [PolicyDomainId], [TenantId]
        from {{ source("assessment_models", "AssessmentPolicyDomainMapping") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentPolicyDomainMapping") }},
    {{ col_rename("AssessmentDomainId", "AssessmentPolicyDomainMapping") }},
    {{ col_rename("PolicyDomainId", "AssessmentPolicyDomainMapping") }},
    {{ col_rename("TenantId", "AssessmentPolicyDomainMapping") }}
from base

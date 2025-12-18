{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [RiskId],
            [AssessmentId],
            [AssessmentDomainProvisionId],
            [AssessmentResponseId]
        from {{ source("assessment_models", "AssessmentDomainProvisionRisk") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentDomainProvisionRisk") }},
    {{ col_rename("TenantId", "AssessmentDomainProvisionRisk") }},
    {{ col_rename("RiskId", "AssessmentDomainProvisionRisk") }},
    {{ col_rename("AssessmentId", "AssessmentDomainProvisionRisk") }},

    {{ col_rename("AssessmentDomainProvisionId", "AssessmentDomainProvisionRisk") }},
    {{ col_rename("AssessmentResponseId", "AssessmentDomainProvisionRisk") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [RiskId],
            [AssessmentId],
            [AssessmentDomainControlId],
            [AssessmentResponseId]
        from {{ source("assessment_models", "AssessmentDomainControlRisk") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentDomainControlRisk") }},
    {{ col_rename("TenantId", "AssessmentDomainControlRisk") }},
    {{ col_rename("RiskId", "AssessmentDomainControlRisk") }},
    {{ col_rename("AssessmentId", "AssessmentDomainControlRisk") }},

    {{ col_rename("AssessmentDomainControlId", "AssessmentDomainControlRisk") }},
    {{ col_rename("AssessmentResponseId", "AssessmentDomainControlRisk") }}
from base

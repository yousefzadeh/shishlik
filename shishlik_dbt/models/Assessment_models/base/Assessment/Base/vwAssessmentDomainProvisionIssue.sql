{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [IssueId],
            [AssessmentId],
            [AssessmentDomainProvisionId],
            [AssessmentResponseId]
        from {{ source("assessment_models", "AssessmentDomainProvisionIssue") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AssessmentDomainProvisionIssue") }},
    {{ col_rename("TenantId", "AssessmentDomainProvisionIssue") }},
    {{ col_rename("IssueId", "AssessmentDomainProvisionIssue") }},
    {{ col_rename("AssessmentId", "AssessmentDomainProvisionIssue") }},

    {{ col_rename("AssessmentDomainProvisionId", "AssessmentDomainProvisionIssue") }},
    {{ col_rename("AssessmentResponseId", "AssessmentDomainProvisionIssue") }}
from base

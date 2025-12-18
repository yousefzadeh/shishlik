{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [IssueId],
            [AssessmentId],
            [AssessmentDomainControlId],
            [AssessmentResponseId]
        from {{ source("assessment_models", "AssessmentDomainControlIssue") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentDomainControlIssue") }},
    {{ col_rename("TenantId", "AssessmentDomainControlIssue") }},
    {{ col_rename("IssueId", "AssessmentDomainControlIssue") }},
    {{ col_rename("AssessmentId", "AssessmentDomainControlIssue") }},

    {{ col_rename("AssessmentDomainControlId", "AssessmentDomainControlIssue") }},
    {{ col_rename("AssessmentResponseId", "AssessmentDomainControlIssue") }}
from base

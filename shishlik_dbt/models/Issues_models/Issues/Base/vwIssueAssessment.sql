{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }},
            [TenantId], 
            [IssueId], 
            [AssessmentId], 
            [AssessmentDomainId], 
            [QuestionId],
            cast(coalesce([LastModificationTime],[CreationTime]) as datetime2) as UpdateTime
        from {{ source("issue_models", "IssueAssessment") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "IssueAssessment") }},
    {{ col_rename("TenantId", "IssueAssessment") }},
    {{ col_rename("IssueId", "IssueAssessment") }},
    {{ col_rename("AssessmentId", "IssueAssessment") }},

    {{ col_rename("AssessmentDomainId", "IssueAssessment") }},
    {{ col_rename("QuestionId", "IssueAssessment") }},
    {{ col_rename("UpdateTime", "IssueAssessment") }}

from
    base

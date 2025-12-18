{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [IssueSubmissionFormId], [UserId], OrganizationUnitId
        from {{ source("issue_models", "IssueSubmissionFormOwner") }} {{ system_remove_IsDeleted() }}
    )

select distinct
    {{ col_rename("Id", "IssueSubmissionFormOwner") }},
    {{ col_rename("CreationTime", "IssueSubmissionFormOwner") }},
    {{ col_rename("LastModificationTime", "IssueSubmissionFormOwner") }},
    {{ col_rename("TenantId", "IssueSubmissionFormOwner") }},
    {{ col_rename("IssueSubmissionFormId", "IssueSubmissionFormOwner") }},
    {{ col_rename("UserId", "IssueSubmissionFormOwner") }},
    {{ col_rename("OrganizationUnitId", "IssueSubmissionFormOwner") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], Name, Description, Status, FormUrl, IsArchived, CreatedFromFormId, TypeId
        from {{ source("issue_models", "IssueSubmissionForm") }} {{ system_remove_IsDeleted() }}
    )

select distinct
    {{ col_rename("Id", "IssueSubmissionForm") }},
    {{ col_rename("TenantId", "IssueSubmissionForm") }},
    {{ col_rename("Name", "IssueSubmissionForm") }},
    {{ col_rename("Description", "IssueSubmissionForm") }},

    {{ col_rename("Status", "IssueSubmissionForm") }},
    {{ col_rename("FormUrl", "IssueSubmissionForm") }},
    {{ col_rename("IsArchived", "IssueSubmissionForm") }},
    {{ col_rename("CreatedFromFormId", "IssueSubmissionForm") }},
    {{ col_rename("TypeId", "IssueSubmissionForm") }}
from base

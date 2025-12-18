{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TagId], [IssueId], [TenantId], CONCAT(TagId, IssueId) as PK
        from {{ source("issue_models", "IssueTag") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "IssueTag") }},
    {{ col_rename("TagId", "IssueTag") }},
    {{ col_rename("IssueId", "IssueTag") }},
    {{ col_rename("TenantId", "IssueTag") }},

    {{ col_rename("PK", "IssueTag") }}
from
    base
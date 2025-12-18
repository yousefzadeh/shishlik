{{ config(materialized="view") }}
--duplicate records
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [IssueId], [AuthorityProvisionId]
        from {{ source("issue_models", "IssueProvision") }} {{ system_remove_IsDeleted() }}
    )

select
    {# {{ col_rename("Id", "IssueProvision") }}, #}
    {{ col_rename("TenantId", "IssueProvision") }},
    {{ col_rename("IssueId", "IssueProvision") }},
    {{ col_rename("AuthorityProvisionId", "IssueProvision") }}
from
    base

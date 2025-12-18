{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [IssueId], [ControlId], [StatementId]
        from {{ source("issue_models", "IssueControlStatement") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "IssueControlStatement") }},
    {{ col_rename("TenantId", "IssueControlStatement") }},
    {{ col_rename("IssueId", "IssueControlStatement") }},
    {{ col_rename("ControlId", "IssueControlStatement") }},

    {{ col_rename("StatementId", "IssueControlStatement") }}
from base

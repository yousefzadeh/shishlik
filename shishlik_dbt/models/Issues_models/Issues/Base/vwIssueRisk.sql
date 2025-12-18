{{ config(materialized="view") }}
--duplicate records
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [IssueId], [RiskId]
        from {{ source("issue_models", "IssueRisk") }} {{ system_remove_IsDeleted() }}
    )

select distinct
    {# {{ col_rename("Id", "IssueRisk") }}, #}
    {{ col_rename("TenantId", "IssueRisk") }},
    {{ col_rename("IssueId", "IssueRisk") }},
    {{ col_rename("RiskId", "IssueRisk") }}
from
    base

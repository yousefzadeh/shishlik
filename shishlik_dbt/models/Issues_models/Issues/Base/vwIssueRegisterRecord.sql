{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [IssueId], [RegisterRecordId], [LinkedIssueId]
        from {{ source("issue_models", "IssueRegisterRecord") }} {{ system_remove_IsDeleted() }}
    )

select distinct
    {# {{ col_rename("Id", "IssueRegisterRecord") }}, #}
    {{ col_rename("TenantId", "IssueRegisterRecord") }},
    {{ col_rename("IssueId", "IssueRegisterRecord") }},
    {{ col_rename("RegisterRecordId", "IssueRegisterRecord") }},
    {{ col_rename("LinkedIssueId", "IssueRegisterRecord") }}
from
    base

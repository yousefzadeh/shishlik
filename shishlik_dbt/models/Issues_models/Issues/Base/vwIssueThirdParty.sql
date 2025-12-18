{{ config(materialized="view") }}
--duplicate records
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [IssueId], [TenantVendorId]
        from {{ source("issue_models", "IssueThirdParty") }} {{ system_remove_IsDeleted() }}
    )

select distinct
    -- {{ col_rename('Id','IssueThirdParty')}},
    {{ col_rename("TenantId", "IssueThirdParty") }},
    {{ col_rename("IssueId", "IssueThirdParty") }},
    {{ col_rename("TenantVendorId", "IssueThirdParty") }}
from
    base

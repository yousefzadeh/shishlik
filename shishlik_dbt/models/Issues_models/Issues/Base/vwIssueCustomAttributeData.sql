{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [IssueId], [ThirdPartyAttributesId]
        from {{ source("issue_models", "IssueCustomAttributeData") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "IssueCustomAttributeData") }},
    {{ col_rename("TenantId", "IssueCustomAttributeData") }},
    {{ col_rename("IssueId", "IssueCustomAttributeData") }},
    {{ col_rename("ThirdPartyAttributesId", "IssueCustomAttributeData") }}
from
    base

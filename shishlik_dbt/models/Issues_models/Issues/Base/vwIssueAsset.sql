{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [IssueId], [AssetId]
        from {{ source("issue_models", "IssueAsset") }} {{ system_remove_IsDeleted() }}
    )

select distinct
    {{ col_rename("Id", "IssueAsset") }},
    {{ col_rename("TenantId", "IssueAsset") }},
    {{ col_rename("IssueId", "IssueAsset") }},
    {{ col_rename("AssetId", "IssueAsset") }}
from base

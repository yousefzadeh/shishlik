{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [IssueId] as AssetId, [RiskId]
        from {{ source("issue_models", "IssueRisk") }}
        join {{ ref("vwAsset")}} a on a.Asset_Id = IssueId
         {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskAsset") }},
    {{ col_rename("CreationTime", "RiskAsset") }},
    {{ col_rename("CreatorUserId", "RiskAsset") }},
    {{ col_rename("LastModificationTime", "RiskAsset") }},

    {{ col_rename("LastModifierUserId", "RiskAsset") }},
    {{ col_rename("IsDeleted", "RiskAsset") }},
    {{ col_rename("DeleterUserId", "RiskAsset") }},
    {{ col_rename("DeletionTime", "RiskAsset") }},

    {{ col_rename("TenantId", "RiskAsset") }},
    {{ col_rename("AssetId", "RiskAsset") }},
    {{ col_rename("RiskId", "RiskAsset") }}
from base

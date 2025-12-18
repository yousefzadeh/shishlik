{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Comment] as nvarchar(4000))[Comment],
            [UserId],
            [RiskId],
            [RootRiskId]
        from {{ source("risk_models", "RiskComment") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskComment") }},
    {{ col_rename("CreationTime", "RiskComment") }},
    {{ col_rename("CreatorUserId", "RiskComment") }},
    {{ col_rename("LastModificationTime", "RiskComment") }},

    {{ col_rename("LastModifierUserId", "RiskComment") }},
    {{ col_rename("IsDeleted", "RiskComment") }},
    {{ col_rename("DeleterUserId", "RiskComment") }},
    {{ col_rename("DeletionTime", "RiskComment") }},

    {{ col_rename("TenantId", "RiskComment") }},
    {{ col_rename("Comment", "RiskComment") }},
    {{ col_rename("UserId", "RiskComment") }},
    {{ col_rename("RiskId", "RiskComment") }},

    {{ col_rename("RootRiskId", "RiskComment") }}
from base

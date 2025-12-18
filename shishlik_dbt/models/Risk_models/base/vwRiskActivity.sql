{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [Type],
            [RiskId],
            [UserId],
            [TenantId],
            cast([ActivityDataStr] as nvarchar(4000)) ActivityDataStr,
            [RootRiskId]
        from {{ source("risk_models", "RiskActivity") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskActivity") }},
    {{ col_rename("CreationTime", "RiskActivity") }},
    {{ col_rename("CreatorUserId", "RiskActivity") }},
    {{ col_rename("LastModificationTime", "RiskActivity") }},

    {{ col_rename("LastModifierUserId", "RiskActivity") }},
    {{ col_rename("IsDeleted", "RiskActivity") }},
    {{ col_rename("DeleterUserId", "RiskActivity") }},
    {{ col_rename("DeletionTime", "RiskActivity") }},

    {{ col_rename("Type", "RiskActivity") }},
    {{ col_rename("RiskId", "RiskActivity") }},
    {{ col_rename("UserId", "RiskActivity") }},
    {{ col_rename("TenantId", "RiskActivity") }},

    {{ col_rename("ActivityDataStr", "RiskActivity") }},
    {{ col_rename("RootRiskId", "RiskActivity") }}
from base

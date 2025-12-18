{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TagId], [RiskId], [TenantId]
        from {{ source("risk_models", "RiskTag") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskTag") }},
    {{ col_rename("CreationTime", "RiskTag") }},
    {{ col_rename("CreatorUserId", "RiskTag") }},
    {{ col_rename("LastModificationTime", "RiskTag") }},

    {{ col_rename("LastModifierUserId", "RiskTag") }},
    {{ col_rename("IsDeleted", "RiskTag") }},
    {{ col_rename("DeleterUserId", "RiskTag") }},
    {{ col_rename("DeletionTime", "RiskTag") }},

    {{ col_rename("TagId", "RiskTag") }},
    {{ col_rename("RiskId", "RiskTag") }},
    {{ col_rename("TenantId", "RiskTag") }}
from base

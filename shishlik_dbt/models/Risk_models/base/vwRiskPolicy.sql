{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [PolicyId], [RiskId], [TenantId]
        from {{ source("risk_models", "RiskPolicy") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskPolicy") }},
    {{ col_rename("CreationTime", "RiskPolicy") }},
    {{ col_rename("CreatorUserId", "RiskPolicy") }},
    {{ col_rename("LastModificationTime", "RiskPolicy") }},

    {{ col_rename("LastModifierUserId", "RiskPolicy") }},
    {{ col_rename("IsDeleted", "RiskPolicy") }},
    {{ col_rename("DeleterUserId", "RiskPolicy") }},
    {{ col_rename("DeletionTime", "RiskPolicy") }},

    {{ col_rename("PolicyId", "RiskPolicy") }},
    {{ col_rename("RiskId", "RiskPolicy") }},
    {{ col_rename("TenantId", "RiskPolicy") }}
from base

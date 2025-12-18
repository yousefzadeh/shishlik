{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [ControlId], [RiskId], [TenantId]
        from {{ source("risk_models", "RiskControl") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskControl") }},
    {{ col_rename("CreationTime", "RiskControl") }},
    {{ col_rename("CreatorUserId", "RiskControl") }},
    coalesce(
    LastModificationTime, CreationTime
    ) as RiskControl_LastModificationTime,
    
    {{ col_rename("LastModifierUserId", "RiskControl") }},
    {{ col_rename("IsDeleted", "RiskControl") }},
    {{ col_rename("DeleterUserId", "RiskControl") }},
    {{ col_rename("DeletionTime", "RiskControl") }},

    {{ col_rename("ControlId", "RiskControl") }},
    {{ col_rename("RiskId", "RiskControl") }},
    {{ col_rename("TenantId", "RiskControl") }}
from base

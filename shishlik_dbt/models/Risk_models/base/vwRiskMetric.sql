{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [MetricId], [RiskId]
        from {{ source("risk_models", "RiskMetric") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskMetric") }},
    {{ col_rename("CreationTime", "RiskMetric") }},
    {{ col_rename("CreatorUserId", "RiskMetric") }},
    {{ col_rename("LastModificationTime", "RiskMetric") }},

    {{ col_rename("LastModifierUserId", "RiskMetric") }},
    {{ col_rename("IsDeleted", "RiskMetric") }},
    {{ col_rename("DeleterUserId", "RiskMetric") }},
    {{ col_rename("DeletionTime", "RiskMetric") }},

    {{ col_rename("TenantId", "RiskMetric") }},
    {{ col_rename("MetricId", "RiskMetric") }},
    {{ col_rename("RiskId", "RiskMetric") }}
from base

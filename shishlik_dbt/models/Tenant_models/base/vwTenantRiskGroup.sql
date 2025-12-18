{{ config(materialized="view") }}

with
    base as (
        select [Id], [CreationTime], [CreatorUserId], [TenantId], [RiskGroupId]
        from {{ source("tenant_models", "TenantRiskGroup") }}
    )

select
    {{ col_rename("Id", "TenantRiskGroup") }},
    {{ col_rename("CreationTime", "TenantRiskGroup") }},
    {{ col_rename("CreatorUserId", "TenantRiskGroup") }},
    {{ col_rename("TenantId", "TenantRiskGroup") }},

    {{ col_rename("RiskGroupId", "TenantRiskGroup") }}
from base

{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [AbstractRiskId], [RiskGroupId]
        from {{ source("grouprisks_models", "GroupRisks") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "GroupRisks") }},
    {{ col_rename("TenantId", "GroupRisks") }},
    {{ col_rename("AbstractRiskId", "GroupRisks") }},
    {{ col_rename("RiskGroupId", "GroupRisks") }}
from base

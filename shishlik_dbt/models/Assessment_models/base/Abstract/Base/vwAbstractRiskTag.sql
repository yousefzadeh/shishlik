{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TagId], [AbstractRiskId], [TenantId]
        from {{ source("assessment_models", "AbstractRiskTag") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AbstractRiskTag") }},
    {{ col_rename("TagId", "AbstractRiskTag") }},
    {{ col_rename("AbstractRiskId", "AbstractRiskTag") }},
    {{ col_rename("TenantId", "AbstractRiskTag") }}
from base

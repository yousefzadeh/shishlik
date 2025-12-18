{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [AbstractRiskId], [IsFavourite]
        from {{ source("assessment_models", "AbstractRiskIsFavourite") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AbstractRiskIsFavourite") }},
    {{ col_rename("TenantId", "AbstractRiskIsFavourite") }},
    {{ col_rename("AbstractRiskId", "AbstractRiskIsFavourite") }},
    {{ col_rename("IsFavourite", "AbstractRiskIsFavourite") }}
from base

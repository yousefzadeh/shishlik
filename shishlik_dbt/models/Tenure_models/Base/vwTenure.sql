{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [Start], [End], [UserId], [PositionId], [TenureClosed], [TenantId]
        from {{ source("tenure_models", "Tenure") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Tenure") }},
    {{ col_rename("Start", "Tenure") }},
    {{ col_rename("End", "Tenure") }},
    {{ col_rename("UserId", "Tenure") }},

    {{ col_rename("PositionId", "Tenure") }},
    {{ col_rename("TenureClosed", "Tenure") }},
    {{ col_rename("TenantId", "Tenure") }}
from base

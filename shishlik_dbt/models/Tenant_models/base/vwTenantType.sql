{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [Type]
        from {{ source("tenant_models", "TenantType") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "TenantType") }},
    {{ col_rename("TenantId", "TenantType") }},
    {{ col_rename("Type", "TenantType") }}
from base

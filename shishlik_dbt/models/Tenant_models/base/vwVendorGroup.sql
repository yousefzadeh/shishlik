{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [Name],
            [Level], 
            [ParentId],
            [TenantId]
        from {{ source("tenant_models", "VendorGroup") }} tv 
        {{ system_remove_IsDeleted() }} 
    )

select
    {{ col_rename("Id", "VendorGroup") }},
    {{ col_rename("Name", "VendorGroup") }},
    {{ col_rename("Level", "VendorGroup") }},
    {{ col_rename("ParentId", "VendorGroup") }},
    {{ col_rename("TenantId", "VendorGroup") }}
from base

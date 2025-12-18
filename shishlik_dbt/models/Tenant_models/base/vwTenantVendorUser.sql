{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [UserId], [TenantVendorId], [TenantId], [UserIdInVendorTenant]
        from {{ source("tenant_models", "TenantVendorUser") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "TenantVendorUser") }},
    {{ col_rename("CreationTime", "TenantVendorUser") }},
    {{ col_rename("LastModificationTime", "TenantVendorUser") }},
    {{ col_rename("UserId", "TenantVendorUser") }},
    {{ col_rename("TenantVendorId", "TenantVendorUser") }},
    {{ col_rename("TenantId", "TenantVendorUser") }},

    {{ col_rename("UserIdInVendorTenant", "TenantVendorUser") }}
from base

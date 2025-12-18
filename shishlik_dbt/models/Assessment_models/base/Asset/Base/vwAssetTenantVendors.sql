{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [AssetId], [TenantVendorId]
        from {{ source("assessment_models", "AssetTenantVendors") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AssetTenantVendors") }},
    {{ col_rename("TenantId", "AssetTenantVendors") }},
    {{ col_rename("AssetId", "AssetTenantVendors") }},
    {{ col_rename("TenantVendorId", "AssetTenantVendors") }}
from base

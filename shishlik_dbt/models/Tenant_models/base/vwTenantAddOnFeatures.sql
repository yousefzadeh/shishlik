{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [AddOnFeatureId], [IsActive]
        from {{ source("tenant_models", "TenantAddOnFeatures") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "TenantAddOnFeatures") }},
    {{ col_rename("TenantId", "TenantAddOnFeatures") }},
    {{ col_rename("AddOnFeatureId", "TenantAddOnFeatures") }},
    {{ col_rename("IsActive", "TenantAddOnFeatures") }}
from base

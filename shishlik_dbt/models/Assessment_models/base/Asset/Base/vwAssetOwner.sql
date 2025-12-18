{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [AssetId], [UserId], [OrganizationUnitId]
        from {{ source("assessment_models", "AssetOwner") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AssetOwner") }},
    {{ col_rename("TenantId", "AssetOwner") }},
    {{ col_rename("AssetId", "AssetOwner") }},
    {{ col_rename("UserId", "AssetOwner") }},

    {{ col_rename("OrganizationUnitId", "AssetOwner") }}
from base

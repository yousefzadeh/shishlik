{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [AssetId], [UserId], [OrganizationUnitId]
        from {{ source("assessment_models", "AssetAccessMember") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AssetAccessMember") }},
    {{ col_rename("TenantId", "AssetAccessMember") }},
    {{ col_rename("AssetId", "AssetAccessMember") }},
    {{ col_rename("UserId", "AssetAccessMember") }},

    {{ col_rename("OrganizationUnitId", "AssetAccessMember") }}
from base

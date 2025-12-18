{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [AssetId], [TagId]
        from {{ source("assessment_models", "AssetTag") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AssetTag") }},
    {{ col_rename("TenantId", "AssetTag") }},
    {{ col_rename("AssetId", "AssetTag") }},
    {{ col_rename("TagId", "AssetTag") }}
from base

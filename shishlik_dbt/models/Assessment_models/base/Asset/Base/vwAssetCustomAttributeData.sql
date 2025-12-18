{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [AssetId], [ThirdPartyAttributesId], [TenantId]
        from {{ source("assessment_models", "AssetCustomAttributeData") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AssetCustomAttributeData") }},
    {{ col_rename("AssetId", "AssetCustomAttributeData") }},
    {{ col_rename("ThirdPartyAttributesId", "AssetCustomAttributeData") }},
    {{ col_rename("TenantId", "AssetCustomAttributeData") }}
from base

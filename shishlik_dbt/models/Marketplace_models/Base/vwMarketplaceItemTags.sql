{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [MarketplaceItemId], [MarketplaceTagsId]
        from {{ source("marketplace_models", "MarketplaceItemTags") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "MarketplaceItemTags") }},
    {{ col_rename("MarketplaceItemId", "MarketplaceItemTags") }},
    {{ col_rename("MarketplaceTagsId", "MarketplaceItemTags") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select [Id], [MarketplaceItemId], [JurisdictionId]
        from {{ source("marketplace_models", "MarketplaceItemJurisdiction") }}
    )

select
    {{ col_rename("Id", "MarketplaceItemJurisdiction") }},
    {{ col_rename("MarketplaceItemId", "MarketplaceItemJurisdiction") }},
    {{ col_rename("JurisdictionId", "MarketplaceItemJurisdiction") }}
from base

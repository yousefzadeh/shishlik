{{ config(materialized="view") }}
with
    base as (
        select [Id], [MarketplaceItemId], [TenantId] from {{ source("marketplace_models", "MarketplaceItemTenant") }}
    )

select
    {{ col_rename("Id", "MarketplaceItemTenant") }},
    {{ col_rename("MarketplaceItemId", "MarketplaceItemTenant") }},
    {{ col_rename("TenantId", "MarketplaceItemTenant") }}
from base

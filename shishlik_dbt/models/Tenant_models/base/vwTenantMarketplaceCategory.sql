{{ config(materialized="view") }}

with
    base as (
        select [Id], [MarketplaceCategoryId], [TenantId], [Enabled]
        from {{ source("tenant_models", "TenantMarketplaceCategory") }}
    )

select
    {{ col_rename("Id", "TenantMarketplaceCategory") }},
    {{ col_rename("MarketplaceCategoryId", "TenantMarketplaceCategory") }},
    {{ col_rename("TenantId", "TenantMarketplaceCategory") }},
    {{ col_rename("Enabled", "TenantMarketplaceCategory") }}
from base

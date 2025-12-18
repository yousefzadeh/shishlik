{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Name] as nvarchar(4000))[Name],
            cast([Color] as nvarchar(4000)) Color,
            cast([Description] as nvarchar(4000)) Description,
            [TenantId]

        from {{ source("marketplace_models", "MarketplaceCategory") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "MarketplaceCategory") }},
    {{ col_rename("Name", "MarketplaceCategory") }},
    {{ col_rename("Color", "MarketplaceCategory") }},
    {{ col_rename("Description", "MarketplaceCategory") }},

    {{ col_rename("TenantId", "MarketplaceCategory") }}
from base

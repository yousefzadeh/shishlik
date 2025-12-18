{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([FileName] as nvarchar(4000)) FileName,
            cast([Fileurl] as nvarchar(4000)) Fileurl,
            [LastUpload],
            [MarketplaceItemId],
            [UserId]
        from {{ source("marketplace_models", "MarketplaceItemDocument") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "MarketplaceItemDocument") }},
    {{ col_rename("TenantId", "MarketplaceItemDocument") }},
    {{ col_rename("FileName", "MarketplaceItemDocument") }},
    {{ col_rename("Fileurl", "MarketplaceItemDocument") }},

    {{ col_rename("LastUpload", "MarketplaceItemDocument") }},
    {{ col_rename("MarketplaceItemId", "MarketplaceItemDocument") }},
    {{ col_rename("UserId", "MarketplaceItemDocument") }}
from base

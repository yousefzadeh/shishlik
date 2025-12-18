{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Name] as nvarchar(4000))[Name],
            cast([Description] as nvarchar(4000)) Description,
            cast([CustomStr] as nvarchar(4000)) CustomStr,
            [Category],
            cast([ShortDescription] as nvarchar(4000)) ShortDescription,
            [MarketplaceCategoryId],
            [Price],
            [TemplateId],
            [ServiceProviderId],
            [TemplateType],
            [TenantId],
            [IsArchived]

        from {{ source("marketplace_models", "MarketplaceItem") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "MarketplaceItem") }},
    {{ col_rename("Name", "MarketplaceItem") }},
    {{ col_rename("Description", "MarketplaceItem") }},
    {{ col_rename("CustomStr", "MarketplaceItem") }},

    {{ col_rename("Category", "MarketplaceItem") }},
    {{ col_rename("ShortDescription", "MarketplaceItem") }},
    {{ col_rename("MarketplaceCategoryId", "MarketplaceItem") }},
    {{ col_rename("Price", "MarketplaceItem") }},

    {{ col_rename("TemplateId", "MarketplaceItem") }},
    {{ col_rename("ServiceProviderId", "MarketplaceItem") }},
    {{ col_rename("TemplateType", "MarketplaceItem") }},
    {{ col_rename("TenantId", "MarketplaceItem") }},

    {{ col_rename("IsArchived", "MarketplaceItem") }}
from base

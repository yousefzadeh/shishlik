{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Name] as nvarchar(4000))[Name],
            cast([Description] as nvarchar(4000)) Description
        from {{ source("marketplace_models", "MarketplaceTags") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "MarketplaceTags") }},
    {{ col_rename("Name", "MarketplaceTags") }},
    {{ col_rename("Description", "MarketplaceTags") }}
from base

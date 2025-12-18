{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Name] as nvarchar(4000))[Name],
            [ContentId],
            [ParentId],
            [Order],
            [TenantId]
        from {{ source("assessment_models", "ContentCategory") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ContentCategory") }},
    {{ col_rename("Name", "ContentCategory") }},
    {{ col_rename("ContentId", "ContentCategory") }},
    {{ col_rename("ParentId", "ContentCategory") }},

    {{ col_rename("Order", "ContentCategory") }},
    {{ col_rename("TenantId", "ContentCategory") }}
from base

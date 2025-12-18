{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Title] as nvarchar(4000)) Title,
            cast([Content] as nvarchar(4000)) Content,
            cast([PopupTitle] as nvarchar(4000)) PopupTitle,
            cast([PopupContent] as nvarchar(4000)) PopupContent,
            [Order],
            [ContentCategoryId],
            [InformationIncluded],
            [TenantId]
        from {{ source("assessment_models", "ContentBlock") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ContentBlock") }},
    {{ col_rename("Title", "ContentBlock") }},
    {{ col_rename("Content", "ContentBlock") }},
    {{ col_rename("PopupTitle", "ContentBlock") }},

    {{ col_rename("PopupContent", "ContentBlock") }},
    {{ col_rename("Order", "ContentBlock") }},
    {{ col_rename("ContentCategoryId", "ContentBlock") }},
    {{ col_rename("InformationIncluded", "ContentBlock") }},

    {{ col_rename("TenantId", "ContentBlock") }}
from base

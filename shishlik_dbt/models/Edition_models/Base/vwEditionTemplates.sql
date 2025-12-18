{{ config(materialized="view") }}

with
    base as (
        select [Id], [CreationTime], [CreatorUserId], [EditionId], [EditionTemplateType], [TemplateId], [TenantId]
        from {{ source("edition_models", "EditionTemplates") }}
    )

select
    {{ col_rename("Id", "EditionTemplates") }},
    {{ col_rename("EditionId", "EditionTemplates") }},
    {{ col_rename("EditionTemplateType", "EditionTemplates") }},
    {{ col_rename("TemplateId", "EditionTemplates") }},

    {{ col_rename("TenantId", "EditionTemplates") }}
from base

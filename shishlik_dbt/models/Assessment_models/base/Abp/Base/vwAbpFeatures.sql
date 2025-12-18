{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            cast([Discriminator] as nvarchar(4000)) Discriminator,
            [Name],
            [Value],
            [EditionId],
            [TenantId]
        from {{ source("assessment_models", "AbpFeatures") }}

    )

select
    {{ col_rename("Id", "AbpFeatures") }},
    {{ col_rename("CreationTime", "AbpFeatures") }},
    {{ col_rename("CreatorUserId", "AbpFeatures") }},
    {{ col_rename("Discriminator", "AbpFeatures") }},

    {{ col_rename("Name", "AbpFeatures") }},
    {{ col_rename("Value", "AbpFeatures") }},
    {{ col_rename("EditionId", "AbpFeatures") }},
    {{ col_rename("TenantId", "AbpFeatures") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [ClaimType],
            cast([ClaimValue] as nvarchar(4000)) ClaimValue,
            [TenantId],
            [UserId]
        from {{ source("assessment_models", "AbpUserClaims") }}
    )

select
    {{ col_rename("Id", "AbpUserClaims") }},
    {{ col_rename("ClaimType", "AbpUserClaims") }},
    {{ col_rename("ClaimValue", "AbpUserClaims") }},
    {{ col_rename("CreationTime", "AbpUserClaims") }},

    {{ col_rename("CreatorUserId", "AbpUserClaims") }},
    {{ col_rename("TenantId", "AbpUserClaims") }},
    {{ col_rename("UserId", "AbpUserClaims") }}
from base

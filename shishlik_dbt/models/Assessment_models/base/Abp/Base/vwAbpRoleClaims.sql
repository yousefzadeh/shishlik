{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [ClaimType],
            cast([ClaimValue] as nvarchar(4000)) ClaimValue,
            [RoleId],
            [TenantId]
        from {{ source("assessment_models", "AbpRoleClaims") }}
    )

select
    {{ col_rename("Id", "AbpRoleClaims") }},
    {{ col_rename("ClaimType", "AbpRoleClaims") }},
    {{ col_rename("ClaimValue", "AbpRoleClaims") }},
    {{ col_rename("RoleId", "AbpRoleClaims") }},

    {{ col_rename("TenantId", "AbpRoleClaims") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            cast([Discriminator] as nvarchar(4000)) Discriminator,
            [IsGranted],
            [Name],
            [TenantId],
            [RoleId],
            [UserId]
        from {{ source("assessment_models", "AbpPermissions") }}
    )

select
    {{ col_rename("Id", "AbpPermissions") }},
    {{ col_rename("Discriminator", "AbpPermissions") }},
    {{ col_rename("IsGranted", "AbpPermissions") }},
    {{ col_rename("Name", "AbpPermissions") }},

    {{ col_rename("TenantId", "AbpPermissions") }},
    {{ col_rename("RoleId", "AbpPermissions") }},
    {{ col_rename("UserId", "AbpPermissions") }}
from base

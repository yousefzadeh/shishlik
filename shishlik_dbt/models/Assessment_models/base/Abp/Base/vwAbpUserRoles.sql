{{ config(materialized="view") }}
with
    base as (
        select [Id], [CreationTime], [CreatorUserId], [RoleId], [TenantId], [UserId]
        from {{ source("assessment_models", "AbpUserRoles") }}
    )

select
    {{ col_rename("Id", "AbpUserRoles") }},
    {{ col_rename("RoleId", "AbpUserRoles") }},
    {{ col_rename("TenantId", "AbpUserRoles") }},
    {{ col_rename("UserId", "AbpUserRoles") }}
from base

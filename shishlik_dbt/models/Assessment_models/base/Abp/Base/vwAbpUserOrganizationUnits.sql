{{ config(materialized="view") }}
with
    base as (
        select [Id], [CreationTime], [CreatorUserId], [IsDeleted], [OrganizationUnitId], [TenantId], [UserId]
        from {{ source("assessment_models", "AbpUserOrganizationUnits") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AbpUserOrganizationUnits") }},
    {{ col_rename("CreationTime", "AbpUserOrganizationUnits") }},
    {{ col_rename("OrganizationUnitId", "AbpUserOrganizationUnits") }},
    {{ col_rename("TenantId", "AbpUserOrganizationUnits") }},
    {{ col_rename("UserId", "AbpUserOrganizationUnits") }}
from base

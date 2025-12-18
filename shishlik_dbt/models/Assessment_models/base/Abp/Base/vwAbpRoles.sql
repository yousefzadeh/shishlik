{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [ConcurrencyStamp],
            [DisplayName],
            [IsDefault],
            [IsStatic],
            [Name],
            [NormalizedName],
            [TenantId],
            [IsHidden]
        from {{ source("assessment_models", "AbpRoles") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AbpRoles") }},
    {{ col_rename("ConcurrencyStamp", "AbpRoles") }},
    {{ col_rename("DisplayName", "AbpRoles") }},
    {{ col_rename("IsDefault", "AbpRoles") }},

    {{ col_rename("IsStatic", "AbpRoles") }},
    {{ col_rename("Name", "AbpRoles") }},
    {{ col_rename("NormalizedName", "AbpRoles") }},
    {{ col_rename("TenantId", "AbpRoles") }},
    {{ col_rename("IsHidden", "AbpRoles") }}
from base

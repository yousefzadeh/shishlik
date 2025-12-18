{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [Icon], [Name], [TenantId], [IsDisabled]
        from {{ source("assessment_models", "AbpLanguages") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AbpLanguages") }},
    {{ col_rename("Icon", "AbpLanguages") }},
    {{ col_rename("Name", "AbpLanguages") }},
    {{ col_rename("TenantId", "AbpLanguages") }},

    {{ col_rename("IsDisabled", "AbpLanguages") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select [Id], [LoginProvider], [ProviderKey], [TenantId], [UserId]
        from {{ source("assessment_models", "AbpUserLogins") }}
    )

select
    {{ col_rename("Id", "AbpUserLogins") }},
    {{ col_rename("LoginProvider", "AbpUserLogins") }},
    {{ col_rename("ProviderKey", "AbpUserLogins") }},
    {{ col_rename("TenantId", "AbpUserLogins") }},

    {{ col_rename("UserId", "AbpUserLogins") }}
from base

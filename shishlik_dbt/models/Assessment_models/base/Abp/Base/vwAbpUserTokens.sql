{{ config(materialized="view") }}
with
    base as (
        select [Id], [LoginProvider], [Name], [TenantId], [UserId], [Value], [ExpireDate]
        from {{ source("assessment_models", "AbpUserTokens") }}
    )

select
    {{ col_rename("Id", "AbpUserTokens") }},
    {{ col_rename("LoginProvider", "AbpUserTokens") }},
    {{ col_rename("Name", "AbpUserTokens") }},
    {{ col_rename("TenantId", "AbpUserTokens") }},

    {{ col_rename("UserId", "AbpUserTokens") }},
    {{ col_rename("Value", "AbpUserTokens") }},
    {{ col_rename("ExpireDate", "AbpUserTokens") }}
from base

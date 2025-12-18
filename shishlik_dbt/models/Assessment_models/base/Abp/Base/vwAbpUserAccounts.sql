{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [EmailAddress],
            [TenantId],
            [UserId],
            [UserLinkId],
            [UserName],
            cast([Discriminator] as nvarchar(4000)) Discriminator,
            [IsAutoCreatedForAdvisor]
        from {{ source("assessment_models", "AbpUserAccounts") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AbpUserAccounts") }},
    {{ col_rename("EmailAddress", "AbpUserAccounts") }},
    {{ col_rename("TenantId", "AbpUserAccounts") }},
    {{ col_rename("UserId", "AbpUserAccounts") }},
    {{ col_rename("UserLinkId", "AbpUserAccounts") }},
    {{ col_rename("UserName", "AbpUserAccounts") }},
    {{ col_rename("Discriminator", "AbpUserAccounts") }},
    {{ col_rename("IsAutoCreatedForAdvisor", "AbpUserAccounts") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [LastModificationTime],
            [LastModifierUserId],
            [Name],
            [TenantId],
            [UserId],
            cast([Value] as nvarchar(4000)) Value
        from {{ source("assessment_models", "AbpSettings") }}
    )

select
    {{ col_rename("Id", "AbpSettings") }},
    {{ col_rename("Name", "AbpSettings") }},
    {{ col_rename("TenantId", "AbpSettings") }},
    {{ col_rename("UserId", "AbpSettings") }},

    {{ col_rename("Value", "AbpSettings") }}
from base

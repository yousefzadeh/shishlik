{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [Key],
            [LanguageName],
            [LastModificationTime],
            [LastModifierUserId],
            [Source],
            [TenantId],
            cast([Value] as nvarchar(4000)) Value
        from {{ source("assessment_models", "AbpLanguageTexts") }}
    )

select
    {{ col_rename("Id", "AbpLanguageTexts") }},
    {{ col_rename("CreationTime", "AbpLanguageTexts") }},
    {{ col_rename("CreatorUserId", "AbpLanguageTexts") }},
    {{ col_rename("Key", "AbpLanguageTexts") }},

    {{ col_rename("LanguageName", "AbpLanguageTexts") }},
    {{ col_rename("LastModificationTime", "AbpLanguageTexts") }},
    {{ col_rename("LastModifierUserId", "AbpLanguageTexts") }},
    {{ col_rename("Source", "AbpLanguageTexts") }},

    {{ col_rename("TenantId", "AbpLanguageTexts") }},
    {{ col_rename("Value", "AbpLanguageTexts") }}
from base

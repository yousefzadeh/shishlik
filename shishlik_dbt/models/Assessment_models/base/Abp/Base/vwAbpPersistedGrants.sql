{{ config(materialized="view") }}
with
    base as (
        select [Id], [ClientId], [CreationTime], cast([Data] as nvarchar(4000)) Data, [Expiration], [SubjectId], [Type]
        from {{ source("assessment_models", "AbpPersistedGrants") }}
    )

select
    {{ col_rename("Id", "AbpPersistedGrants") }},
    {{ col_rename("ClientId", "AbpPersistedGrants") }},
    {{ col_rename("CreationTime", "AbpPersistedGrants") }},
    {{ col_rename("Data", "AbpPersistedGrants") }},

    {{ col_rename("Expiration", "AbpPersistedGrants") }},
    {{ col_rename("SubjectId", "AbpPersistedGrants") }},
    {{ col_rename("Type", "AbpPersistedGrants") }}
from base

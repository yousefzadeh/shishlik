{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            cast([EmailAddress] as nvarchar(4000)) EmailAddress,
            [TenantId],
            [EmailType],
            [RecipientId],
            [IsSent],
            [EmailEntityType],
            [EntityId],
            [RecipientTenantId]
        from {{ source("periodicemaillog_models", "PeriodicEmailLog") }}
    )

select
    {{ col_rename("Id", "PeriodicEmailLog") }},
    {{ col_rename("CreationTime", "PeriodicEmailLog") }},
    {{ col_rename("CreatorUserId", "PeriodicEmailLog") }},
    {{ col_rename("EmailAddress", "PeriodicEmailLog") }},

    {{ col_rename("TenantId", "PeriodicEmailLog") }},
    {{ col_rename("EmailType", "PeriodicEmailLog") }},
    {{ col_rename("RecipientId", "PeriodicEmailLog") }},
    {{ col_rename("IsSent", "PeriodicEmailLog") }},

    {{ col_rename("EmailEntityType", "PeriodicEmailLog") }},
    {{ col_rename("EntityId", "PeriodicEmailLog") }},
    {{ col_rename("RecipientTenantId", "PeriodicEmailLog") }}
from base

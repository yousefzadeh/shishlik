{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [Status],
            cast([Error] as nvarchar(4000)) Error,
            cast([ErrorStackTrace] as nvarchar(4000)) ErrorStackTrace,
            cast([BackgroundJobId] as nvarchar(4000)) BackgroundJobId,
            [GraphDbEntityType]
        from {{ source("CosmosDbBackgroundJobLog_models", "CosmosDbBackgroundJobLog") }}
    )

select
    {{ col_rename("Id", "CosmosDbBackgroundJobLog") }},
    {{ col_rename("CreationTime", "CosmosDbBackgroundJobLog") }},
    {{ col_rename("CreatorUserId", "CosmosDbBackgroundJobLog") }},
    {{ col_rename("Status", "CosmosDbBackgroundJobLog") }},

    {{ col_rename("Error", "CosmosDbBackgroundJobLog") }},
    {{ col_rename("ErrorStackTrace", "CosmosDbBackgroundJobLog") }},
    {{ col_rename("BackgroundJobId", "CosmosDbBackgroundJobLog") }},
    {{ col_rename("GraphDbEntityType", "CosmosDbBackgroundJobLog") }}
from base

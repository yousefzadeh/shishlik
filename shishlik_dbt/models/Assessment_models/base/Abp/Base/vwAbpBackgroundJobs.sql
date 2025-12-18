{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [IsAbandoned],
            cast([JobArgs] as nvarchar(4000)) JobArgs,
            [JobType],
            [LastTryTime],
            [NextTryTime],
            [Priority],
            [TryCount]
        from {{ source("assessment_models", "AbpBackgroundJobs") }}
    )

select
    {{ col_rename("Id", "AbpBackgroundJobs") }},
    {{ col_rename("IsAbandoned", "AbpBackgroundJobs") }},
    {{ col_rename("JobArgs", "AbpBackgroundJobs") }},
    {{ col_rename("JobType", "AbpBackgroundJobs") }},

    {{ col_rename("LastTryTime", "AbpBackgroundJobs") }},
    {{ col_rename("NextTryTime", "AbpBackgroundJobs") }},
    {{ col_rename("Priority", "AbpBackgroundJobs") }},
    {{ col_rename("TryCount", "AbpBackgroundJobs") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [BrowserInfo],
            [ClientIpAddress],
            [ClientName],
            [Result],
            [TenancyName],
            [TenantId],
            [UserId],
            [UserNameOrEmailAddress]
        from {{ source("assessment_models", "AbpUserLoginAttempts") }}
    )

select
    {{ col_rename("Id", "AbpUserLoginAttempts") }},
    {{ col_rename("BrowserInfo", "AbpUserLoginAttempts") }},
    {{ col_rename("ClientIpAddress", "AbpUserLoginAttempts") }},
    {{ col_rename("ClientName", "AbpUserLoginAttempts") }},

    {{ col_rename("CreationTime", "AbpUserLoginAttempts") }},
    DATEADD(mi, DATEDIFF(mi, 0, CreationTime), 0) AbpUserLoginAttempts_DateTime,
    cast(Format(CreationTime, 'MMM, yyyy') as varchar) AbpUserLoginAttempts_CreationTimeMonth,
    {{ col_rename("Result", "AbpUserLoginAttempts") }},
    {{ col_rename("TenancyName", "AbpUserLoginAttempts") }},
    {{ col_rename("TenantId", "AbpUserLoginAttempts") }},

    {{ col_rename("UserId", "AbpUserLoginAttempts") }},
    {{ col_rename("UserNameOrEmailAddress", "AbpUserLoginAttempts") }}
from base

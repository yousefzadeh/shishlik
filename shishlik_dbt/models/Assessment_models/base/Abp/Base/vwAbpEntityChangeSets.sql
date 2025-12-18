{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [BrowserInfo],
            [ClientIpAddress],
            [ClientName],
            [CreationTime],
            cast([ExtensionData] as nvarchar(4000)) ExtensionData,
            [ImpersonatorTenantId],
            [ImpersonatorUserId],
            [Reason],
            [TenantId],
            [UserId]
        from {{ source("assessment_models", "AbpEntityChangeSets") }}
    )

select
    {{ col_rename("Id", "AbpEntityChangeSets") }},
    {{ col_rename("BrowserInfo", "AbpEntityChangeSets") }},
    {{ col_rename("ClientIpAddress", "AbpEntityChangeSets") }},
    {{ col_rename("ClientName", "AbpEntityChangeSets") }},

    {{ col_rename("CreationTime", "AbpEntityChangeSets") }},
    {{ col_rename("ExtensionData", "AbpEntityChangeSets") }},
    {{ col_rename("ImpersonatorTenantId", "AbpEntityChangeSets") }},
    {{ col_rename("ImpersonatorUserId", "AbpEntityChangeSets") }},

    {{ col_rename("Reason", "AbpEntityChangeSets") }},
    {{ col_rename("TenantId", "AbpEntityChangeSets") }},
    {{ col_rename("UserId", "AbpEntityChangeSets") }}
from base

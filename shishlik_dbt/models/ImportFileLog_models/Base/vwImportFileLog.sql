{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            cast([FileName] as nvarchar(4000)) FileName,
            cast([FilePhysicalPath] as nvarchar(4000)) FilePhysicalPath,
            [Status],
            cast([Error] as nvarchar(4000)) Error,
            [TenantId],
            cast([BackgroundJobId] as nvarchar(4000)) BackgroundJobId,
            [ImportEntityType],
            cast([ErrorStackTrace] as nvarchar(4000)) ErrorStackTrace,
            [RegisterId]
        from {{ source("importfilelog_models", "ImportFileLog") }}
    )

select
    {{ col_rename("Id", "ImportFileLog") }},
    {{ col_rename("CreationTime", "ImportFileLog") }},
    {{ col_rename("CreatorUserId", "ImportFileLog") }},
    {{ col_rename("FileName", "ImportFileLog") }},

    {{ col_rename("FilePhysicalPath", "ImportFileLog") }},
    {{ col_rename("Status", "ImportFileLog") }},
    {{ col_rename("Error", "ImportFileLog") }},
    {{ col_rename("TenantId", "ImportFileLog") }},

    {{ col_rename("BackgroundJobId", "ImportFileLog") }},
    {{ col_rename("ImportEntityType", "ImportFileLog") }},
    {{ col_rename("ErrorStackTrace", "ImportFileLog") }},
    {{ col_rename("RegisterId", "ImportFileLog") }}
from base

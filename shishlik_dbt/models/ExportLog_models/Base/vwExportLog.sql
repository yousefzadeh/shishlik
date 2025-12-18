{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            cast([FileName] as nvarchar(4000)) FileName,
            cast([FileUrl] as nvarchar(4000)) FileUrl,
            [Status],
            cast([Error] as nvarchar(4000)) Error,
            cast([ErrorStackTrace] as nvarchar(4000)) ErrorStackTrace,
            [TenantId],
            cast([BackgroundJobId] as nvarchar(4000)) BackgroundJobId,
            [ExportType],
            cast([ExportInputParamsJson] as nvarchar(4000)) ExportInputParamsJson,
            [FileCreationTime]
        from {{ source("exportlog_models", "ExportLog") }}
    )

select
    {{ col_rename("Id", "ExportLog") }},
    {{ col_rename("CreationTime", "ExportLog") }},
    {{ col_rename("CreatorUserId", "ExportLog") }},
    {{ col_rename("FileName", "ExportLog") }},

    {{ col_rename("FileUrl", "ExportLog") }},
    {{ col_rename("Status", "ExportLog") }},
    {{ col_rename("Error", "ExportLog") }},
    {{ col_rename("ErrorStackTrace", "ExportLog") }},

    {{ col_rename("TenantId", "ExportLog") }},
    {{ col_rename("BackgroundJobId", "ExportLog") }},
    {{ col_rename("ExportType", "ExportLog") }},
    {{ col_rename("ExportInputParamsJson", "ExportLog") }},

    {{ col_rename("FileCreationTime", "ExportLog") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Name] as varchar(200)) as [Name],
            cast([Url] as nvarchar(4000)) Url,
            cast([Type] as nvarchar(4000))[Type],
            cast([Description] as nvarchar(4000)) Description,
            [LastUpdated],
            cast([FileName] as nvarchar(4000)) FileName,
            cast([Fileurl] as nvarchar(4000)) Fileurl,
            [IsUploadedToHailey],
            cast([Body] as nvarchar(4000)) Body,
            cast([AuthoritySector] as nvarchar(4000)) AuthoritySector,
            [JurisdictionId],
            [ArchivedDate],
            [IsArchived],
            [LastPublishedTime],
            [Status],
            case when [Status] = 1 then 'Edit' when [Status] = 2 then 'Published' else 'Undefined' end as [StatusCode],
            [TenantId],
            [CreatedFromAuthorityId],
            coalesce(LastModificationTime, CreationTime) as UpdateTime
        from {{ source("assessment_models", "Authority") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Authority") }},
    {{ col_rename("Name", "Authority") }},
    {{ col_rename("Url", "Authority") }},
    {{ col_rename("Type", "Authority") }},

    {{ col_rename("Description", "Authority") }},
    {{ col_rename("LastUpdated", "Authority") }},
    {{ col_rename("FileName", "Authority") }},
    {{ col_rename("Fileurl", "Authority") }},

    {{ col_rename("IsUploadedToHailey", "Authority") }},
    {{ col_rename("Body", "Authority") }},
    {{ col_rename("AuthoritySector", "Authority") }},
    {{ col_rename("JurisdictionId", "Authority") }},

    {{ col_rename("ArchivedDate", "Authority") }},
    {{ col_rename("IsArchived", "Authority") }},
    {{ col_rename("LastPublishedTime", "Authority") }},
    {{ col_rename("Status", "Authority") }},
    {{ col_rename("StatusCode", "Authority") }},

    {{ col_rename("TenantId", "Authority") }},
    {{ col_rename("CreatedFromAuthorityId", "Authority") }},
    {{ col_rename("CreationTime", "Authority") }},
    {{ col_rename("UpdateTime", "Authority") }}
from base

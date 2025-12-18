{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [TenantId],
            [ThirdPartyDynamicFieldConfigurationId],
            cast([FileName] as nvarchar(4000)) FileName,
            cast([DisplayFileName] as nvarchar(4000)) DisplayFileName,
            cast([FileUrl] as nvarchar(4000)) FileUrl,
            [FileSizeInKB],
            [Active]
        from {{ source("issue_models", "ThirdPartyDyanmicFieldDataFileLog") }}
    )

select
    {{ col_rename("Id", "ThirdPartyDyanmicFieldDataFileLog") }},
    {{ col_rename("CreationTime", "ThirdPartyDyanmicFieldDataFileLog") }},
    {{ col_rename("CreatorUserId", "ThirdPartyDyanmicFieldDataFileLog") }},
    {{ col_rename("TenantId", "ThirdPartyDyanmicFieldDataFileLog") }},

    {{ col_rename("ThirdPartyDynamicFieldConfigurationId", "ThirdPartyDyanmicFieldDataFileLog") }},
    {{ col_rename("FileName", "ThirdPartyDyanmicFieldDataFileLog") }},
    {{ col_rename("DisplayFileName", "ThirdPartyDyanmicFieldDataFileLog") }},
    {{ col_rename("FileUrl", "ThirdPartyDyanmicFieldDataFileLog") }},

    {{ col_rename("FileSizeInKB", "ThirdPartyDyanmicFieldDataFileLog") }},
    {{ col_rename("Active", "ThirdPartyDyanmicFieldDataFileLog") }}
from base

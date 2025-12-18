{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [TenantId],
            cast([Name] as nvarchar(4000))[Name],
            cast([Type] as nvarchar(4000))[Type],
            cast([AssociatedThirdParty] as nvarchar(4000)) AssociatedThirdParty,
            cast([Owner] as nvarchar(4000)) Owner,
            cast([CustomAttributeJson] as nvarchar(4000)) CustomAttributeJson,
            [HasError],
            cast([ErrorMessage] as nvarchar(4000)) ErrorMessage,
            [IsExists],
            [IsDuplicate],
            [IsImported],
            [ImportFileLogId],
            cast([Description] as nvarchar(4000)) Description,
            cast([Tags] as nvarchar(4000)) Tags
        from {{ source("register_models", "RegisterImportStagingRecord") }}
    )

select
    {{ col_rename("Id", "RegisterImportStagingRecord") }},
    {{ col_rename("TenantId", "RegisterImportStagingRecord") }},
    {{ col_rename("Name", "RegisterImportStagingRecord") }},
    {{ col_rename("Type", "RegisterImportStagingRecord") }},

    {{ col_rename("AssociatedThirdParty", "RegisterImportStagingRecord") }},
    {{ col_rename("Owner", "RegisterImportStagingRecord") }},
    {{ col_rename("CustomAttributeJson", "RegisterImportStagingRecord") }},
    {{ col_rename("HasError", "RegisterImportStagingRecord") }},

    {{ col_rename("ErrorMessage", "RegisterImportStagingRecord") }},
    {{ col_rename("IsExists", "RegisterImportStagingRecord") }},
    {{ col_rename("IsDuplicate", "RegisterImportStagingRecord") }},
    {{ col_rename("IsImported", "RegisterImportStagingRecord") }},

    {{ col_rename("ImportFileLogId", "RegisterImportStagingRecord") }},
    {{ col_rename("Description", "RegisterImportStagingRecord") }},
    {{ col_rename("Tags", "RegisterImportStagingRecord") }}
from base

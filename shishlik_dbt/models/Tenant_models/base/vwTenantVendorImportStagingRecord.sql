{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            cast([VendorName] as nvarchar(4000)) VendorName,
            cast([ContactEmail] as nvarchar(4000)) ContactEmail,
            [HasError],
            cast([ErrorMessage] as nvarchar(4000)) ErrorMessage,
            [IsExists],
            [IsDuplicate],
            [TenantId],
            [ImportFileLogId],
            [IsImported],
            cast([TenancyName] as nvarchar(4000)) TenancyName,
            cast([Criticality] as nvarchar(4000)) Criticality,
            cast([CustomData] as nvarchar(4000)) CustomData,
            cast([Geography] as nvarchar(4000)) Geography,
            cast([Industry] as nvarchar(4000)) Industry,
            cast([InherentRisk] as nvarchar(4000)) InherentRisk,
            cast([Tags] as nvarchar(4000)) Tags,
            cast([Website] as nvarchar(4000)) Website
        from {{ source("tenant_models", "TenantVendorImportStagingRecord") }}
    )

select
    {{ col_rename("Id", "TenantVendorImportStagingRecord") }},
    {{ col_rename("VendorName", "TenantVendorImportStagingRecord") }},
    {{ col_rename("ContactEmail", "TenantVendorImportStagingRecord") }},
    {{ col_rename("HasError", "TenantVendorImportStagingRecord") }},

    {{ col_rename("ErrorMessage", "TenantVendorImportStagingRecord") }},
    {{ col_rename("IsExists", "TenantVendorImportStagingRecord") }},
    {{ col_rename("IsDuplicate", "TenantVendorImportStagingRecord") }},
    {{ col_rename("TenantId", "TenantVendorImportStagingRecord") }},

    {{ col_rename("ImportFileLogId", "TenantVendorImportStagingRecord") }},
    {{ col_rename("IsImported", "TenantVendorImportStagingRecord") }},
    {{ col_rename("TenancyName", "TenantVendorImportStagingRecord") }},
    {{ col_rename("Criticality", "TenantVendorImportStagingRecord") }},

    {{ col_rename("CustomData", "TenantVendorImportStagingRecord") }},
    {{ col_rename("Geography", "TenantVendorImportStagingRecord") }},
    {{ col_rename("Industry", "TenantVendorImportStagingRecord") }},
    {{ col_rename("InherentRisk", "TenantVendorImportStagingRecord") }},

    {{ col_rename("Tags", "TenantVendorImportStagingRecord") }},
    {{ col_rename("Website", "TenantVendorImportStagingRecord") }}
from base

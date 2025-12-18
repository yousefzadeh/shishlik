{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            cast([DocumentFileName] as nvarchar(4000)) DocumentFileName,
            cast([DisplayFileName] as nvarchar(4000)) DisplayFileName,
            cast([DocumentUrl] as nvarchar(4000)) DocumentUrl,
            [FileSizeInKB],
            [TenantId],
            [TenantVendorId]
        from {{ source("tenant_models", "TenantVendorDocument") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "TenantVendorDocument") }},
    {{ col_rename("DocumentFileName", "TenantVendorDocument") }},
    {{ col_rename("DisplayFileName", "TenantVendorDocument") }},
    {{ col_rename("DocumentUrl", "TenantVendorDocument") }},

    {{ col_rename("FileSizeInKB", "TenantVendorDocument") }},
    {{ col_rename("TenantId", "TenantVendorDocument") }},
    {{ col_rename("TenantVendorId", "TenantVendorDocument") }}
from base

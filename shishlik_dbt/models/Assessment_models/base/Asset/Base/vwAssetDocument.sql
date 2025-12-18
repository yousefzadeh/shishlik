{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([DocumentFileName] as nvarchar(4000)) DocumentFileName,
            cast([DisplayFileName] as nvarchar(4000)) DisplayFileName,
            cast([DocumentUrl] as nvarchar(4000)) DocumentUrl,
            [FileSizeInKB],
            [AssetId]
        from {{ source("assessment_models", "AssetDocument") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AssetDocument") }},
    {{ col_rename("TenantId", "AssetDocument") }},
    {{ col_rename("DocumentFileName", "AssetDocument") }},
    {{ col_rename("DisplayFileName", "AssetDocument") }},

    {{ col_rename("DocumentUrl", "AssetDocument") }},
    {{ col_rename("FileSizeInKB", "AssetDocument") }},
    {{ col_rename("AssetId", "AssetDocument") }}
from base

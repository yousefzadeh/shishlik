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
            [RegisterRecordId]
        from {{ source("register_models", "RegisterRecordDocument") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RegisterRecordDocument") }},
    {{ col_rename("TenantId", "RegisterRecordDocument") }},
    {{ col_rename("DocumentFileName", "RegisterRecordDocument") }},
    {{ col_rename("DisplayFileName", "RegisterRecordDocument") }},

    {{ col_rename("DocumentUrl", "RegisterRecordDocument") }},
    {{ col_rename("FileSizeInKB", "RegisterRecordDocument") }},
    {{ col_rename("RegisterRecordId", "RegisterRecordDocument") }}
from base

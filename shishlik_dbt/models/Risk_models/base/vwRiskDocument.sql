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
            [RiskId]
        from {{ source("risk_models", "RiskDocument") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskDocument") }},
    {{ col_rename("CreationTime", "RiskDocument") }},
    {{ col_rename("CreatorUserId", "RiskDocument") }},
    {{ col_rename("LastModificationTime", "RiskDocument") }},

    {{ col_rename("LastModifierUserId", "RiskDocument") }},
    {{ col_rename("IsDeleted", "RiskDocument") }},
    {{ col_rename("DeleterUserId", "RiskDocument") }},
    {{ col_rename("DeletionTime", "RiskDocument") }},

    {{ col_rename("TenantId", "RiskDocument") }},
    {{ col_rename("DocumentFileName", "RiskDocument") }},
    {{ col_rename("DisplayFileName", "RiskDocument") }},
    {{ col_rename("DocumentUrl", "RiskDocument") }},

    {{ col_rename("FileSizeInKB", "RiskDocument") }},
    {{ col_rename("RiskId", "RiskDocument") }}
from base

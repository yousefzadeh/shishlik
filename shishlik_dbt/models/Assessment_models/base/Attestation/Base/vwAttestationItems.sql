{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [ItemId],
            [ItemEntityType],
            case
                when [ItemEntityType] = 1 then 'Controls ' when [ItemEntityType] = 2 then 'Risk' else 'Undefined'
            end as [ItemEntityTypeCode],
            cast([DisplayName] as nvarchar(4000)) DisplayName,
            [AttestationId],
            [TenantId],
            [Version],
            [IsNewVersionAvailable],
            [VersionDate],
            [LatestVersionItemId]
        from {{ source("assessment_models", "AttestationItems") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AttestationItems") }},
    {{ col_rename("ItemId", "AttestationItems") }},
    {{ col_rename("ItemEntityType", "AttestationItems") }},
    {{ col_rename("ItemEntityTypeCode", "AttestationItems") }},
    {{ col_rename("DisplayName", "AttestationItems") }},

    {{ col_rename("AttestationId", "AttestationItems") }},
    {{ col_rename("TenantId", "AttestationItems") }},
    {{ col_rename("Version", "AttestationItems") }},
    {{ col_rename("IsNewVersionAvailable", "AttestationItems") }},

    {{ col_rename("VersionDate", "AttestationItems") }},
    {{ col_rename("LatestVersionItemId", "AttestationItems") }}
from base

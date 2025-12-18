{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Name] as nvarchar(4000))[Name],
            cast([Description] as nvarchar(4000)) Description,
            [JurisdictionId]
        from {{ source("assessment_models", "Content") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Content") }},
    {{ col_rename("TenantId", "Content") }},
    {{ col_rename("Name", "Content") }},
    {{ col_rename("Description", "Content") }},

    {{ col_rename("JurisdictionId", "Content") }}
from base

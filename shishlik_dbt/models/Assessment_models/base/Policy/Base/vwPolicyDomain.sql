{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Name] as nvarchar(4000))[Name],
            cast([Custom] as nvarchar(4000)) Custom,
            [PolicyId],
            -- ,[ControlsId] -- all NULL
            [TenantId]
        from {{ source("assessment_models", "PolicyDomain") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "PolicyDomain") }},
    {{ col_rename("Name", "PolicyDomain") }},
    {{ col_rename("Custom", "PolicyDomain") }},
    {{ col_rename("PolicyId", "PolicyDomain") }},

    {{ col_rename("TenantId", "PolicyDomain") }}
from base

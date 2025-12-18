{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Name] as nvarchar(4000))[Name],
            cast([Description] as nvarchar(4000)) Description,
            [JurisdictionId],
            cast([OrgChartJsonData] as nvarchar(4000)) OrgChartJsonData
        from {{ source("Team_models", "Team") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Team") }},
    {{ col_rename("TenantId", "Team") }},
    {{ col_rename("Name", "Team") }},
    {{ col_rename("Description", "Team") }},

    {{ col_rename("JurisdictionId", "Team") }},
    {{ col_rename("OrgChartJsonData", "Team") }}
from base

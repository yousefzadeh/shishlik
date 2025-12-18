{{ config(materialized="view") }}

with
    base as (
        select [Id], [CreationTime], [CreatorUserId], [RoleId], [SuperiorId], [StartDate], [EndDate], [TenantId]
        from {{ source("Report_models", "ReportsTo") }}
    )

select
    {{ col_rename("Id", "ReportsTo") }},
    {{ col_rename("CreationTime", "ReportsTo") }},
    {{ col_rename("CreatorUserId", "ReportsTo") }},
    {{ col_rename("RoleId", "ReportsTo") }},

    {{ col_rename("SuperiorId", "ReportsTo") }},
    {{ col_rename("StartDate", "ReportsTo") }},
    {{ col_rename("EndDate", "ReportsTo") }},
    {{ col_rename("TenantId", "ReportsTo") }}
from base

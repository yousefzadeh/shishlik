{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Title] as nvarchar(4000)) Title,
            cast([Group] as nvarchar(4000))[Group],
            [TeamId],
            [PositionCreated],
            [PositionDeprecated]
        from {{ source("position_models", "Position") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Position") }},
    {{ col_rename("TenantId", "Position") }},
    {{ col_rename("Title", "Position") }},
    {{ col_rename("Group", "Position") }},

    {{ col_rename("TeamId", "Position") }},
    {{ col_rename("PositionCreated", "Position") }},
    {{ col_rename("PositionDeprecated", "Position") }}
from base

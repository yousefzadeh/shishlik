{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Subject] as nvarchar(4000)) Subject,
            [StartTime],
            [EndTime],
            [IsFullDay]
        from {{ source("assessment_models", "CalendarEvents") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "CalendarEvents") }},
    {{ col_rename("TenantId", "CalendarEvents") }},
    {{ col_rename("Subject", "CalendarEvents") }},
    {{ col_rename("StartTime", "CalendarEvents") }},

    {{ col_rename("EndTime", "CalendarEvents") }},
    {{ col_rename("IsFullDay", "CalendarEvents") }}
from base

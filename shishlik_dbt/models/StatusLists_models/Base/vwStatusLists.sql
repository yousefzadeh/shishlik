{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            case
                when cast([Name] as nvarchar(4000)) is NULL
                then 'Not Selected'
                when cast([Name] as nvarchar(4000)) = ''
                then 'Not Selected'
                else cast([Name] as nvarchar(4000))
            end
            [NAME],
            [Reference],
            [IsClosedActionStatus],
            [StatusOrder]
        from {{ source("statuslists_models", "StatusLists") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "StatusLists") }},
    {{ col_rename("TenantId", "StatusLists") }},
    {{ col_rename("Name", "StatusLists") }},
    {{ col_rename("Reference", "StatusLists") }},

    {{ col_rename("IsClosedActionStatus", "StatusLists") }},
    {{ col_rename("StatusOrder", "StatusLists") }}
from base

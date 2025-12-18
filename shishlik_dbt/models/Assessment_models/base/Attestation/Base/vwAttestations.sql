{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Name] as nvarchar(4000))[Name],
            cast([Description] as nvarchar(4000)) Description,
            [DueDate],
            [Status],
            case
                when [Status] = 1
                then 'Edit'
                when [Status] = 2
                then 'Sent'
                when [Status] = 3
                then 'Closed Before Completion'
                when [Status] = 4
                then 'Completed'
                else 'Undefined'
            end as [StatusCode],
            [TenantId],
            [IsArchived]
        from {{ source("assessment_models", "Attestations") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Attestations") }},
    {{ col_rename("Name", "Attestations") }},
    {{ col_rename("Description", "Attestations") }},
    {{ col_rename("DueDate", "Attestations") }},

    {{ col_rename("Status", "Attestations") }},
    {{ col_rename("StatusCode", "Attestations") }},
    {{ col_rename("TenantId", "Attestations") }},
    {{ col_rename("IsArchived", "Attestations") }}
from base

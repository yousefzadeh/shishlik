{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Title] as nvarchar(4000)) Title,
            cast([Description] as nvarchar(4000)) Description,
            [DueDate],
            [Time],
            [Status],
            [PublishedDate],
            [HasPeriod],
            [Period],
            [PeriodicStatementId],
            [PeriodStartDate],
            [TemplateStatementId],
            [Version],
            [ParentStatementId],
            [RootStatementId],
            [Order],
            [ActionStatus],
            case
                when [ActionStatus] = 0 then 'New' when [ActionStatus] = 1 then 'Completed' else 'Undefined'
            end as ActionStatusCode,
            {{ IsCurrentRow("RootStatementId") }}
        from {{ source("statement_models", "Statement") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Statement") }},
    {{ col_rename("TenantId", "Statement") }},
    {{ col_rename("Title", "Statement") }},
    {{ col_rename("Description", "Statement") }},

    {{ col_rename("DueDate", "Statement") }},
    {{ col_rename("Time", "Statement") }},
    {{ col_rename("Status", "Statement") }},
    {{ col_rename("PublishedDate", "Statement") }},

    {{ col_rename("HasPeriod", "Statement") }},
    {{ col_rename("Period", "Statement") }},
    {{ col_rename("PeriodicStatementId", "Statement") }},
    {{ col_rename("PeriodStartDate", "Statement") }},

    {{ col_rename("TemplateStatementId", "Statement") }},
    {{ col_rename("Version", "Statement") }},
    {{ col_rename("ParentStatementId", "Statement") }},
    {{ col_rename("RootStatementId", "Statement") }},

    {{ col_rename("Order", "Statement") }},
    {{ col_rename("ActionStatus", "Statement") }},
    {{ col_rename("ActionStatusCode", "Statement") }},
    {{ col_rename("IsCurrent", "Statement") }}

from base

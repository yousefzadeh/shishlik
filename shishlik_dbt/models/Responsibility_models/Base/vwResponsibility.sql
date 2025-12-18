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
            case
                when [Status] = 1
                then 'Edit'
                when [Status] = 2
                then 'Published'
                when [Status] = 100
                then 'Deprecated'
                else 'Undefined'
            end StatusCode,
            [PublishedDate],
            COALESCE([HasPeriod], 0) HasPeriod,
            [Period],
            case
                when [Period] = 1
                then 'Every Year'
                when [Period] = 2
                then 'Every 6 Months'
                when [Period] = 3
                then 'Every Month'
                when [Period] = 4
                then 'Every Week'
                when [Period] = 6
                then 'Every 3 Months'
                when [Period] = 7
                then 'Every Day'
                else 'Once-off'
            end PeriodCode,
            [PeriodicStatementId] PeriodicResponsibilityId,
            [PeriodStartDate],
            [TemplateStatementId] TemplateResponsibilityId,
            case when [TemplateStatementId] is null then 1 else 0 end IsTemplate,
            [Version],
            [ParentStatementId] ParentResponsibilityId,
            case when [ParentStatementId] is NULL then 1 else 0 end IsRoot,
            COALESCE([RootStatementId], [Id]) RootResponsibilityId,
            [Order],
            [ActionStatus],
            case
                when [ActionStatus] = 0 then 'New' when [ActionStatus] = 1 then 'Completed' else 'Undefined'
            end as ActionStatusCode,
            {{ IsCurrentRow("RootStatementId") }}
        from {{ source("statement_models", "Statement") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Responsibility") }},
    {{ col_rename("TenantId", "Responsibility") }},
    {{ col_rename("Title", "Responsibility") }},
    {{ col_rename("Description", "Responsibility") }},

    {{ col_rename("DueDate", "Responsibility") }},
    {{ col_rename("Time", "Responsibility") }},
    {{ col_rename("Status", "Responsibility") }},
    {{ col_rename("StatusCode", "Responsibility") }},
    {{ col_rename("PublishedDate", "Responsibility") }},

    {{ col_rename("HasPeriod", "Responsibility") }},
    {{ col_rename("Period", "Responsibility") }},
    {{ col_rename("PeriodCode", "Responsibility") }},
    {{ col_rename("PeriodicResponsibilityId", "Responsibility") }},
    {{ col_rename("PeriodStartDate", "Responsibility") }},

    {{ col_rename("TemplateResponsibilityId", "Responsibility") }},
    {{ col_rename("Version", "Responsibility") }},
    {{ col_rename("ParentResponsibilityId", "Responsibility") }},
    {{ col_rename("RootResponsibilityId", "Responsibility") }},

    {{ col_rename("Order", "Responsibility") }},
    {{ col_rename("ActionStatus", "Responsibility") }},
    {{ col_rename("ActionStatusCode", "Responsibility") }},
    {{ col_rename("IsCurrent", "Responsibility") }},
    {{ col_rename("IsTemplate", "Responsibility") }},
    {{ col_rename("IsRoot", "Responsibility") }}

from base

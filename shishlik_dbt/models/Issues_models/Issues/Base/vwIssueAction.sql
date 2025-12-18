{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Title] as nvarchar(4000)) Title,
            [DueDate],
            [CompletedDate],
            case
                when Status in (1)
                then 'Not Overdue'
                when Status in (0, 3) and getdate() <= DueDate
                then 'Not Overdue'
                when getdate() > DueDate
                then 'Overdue'
                else 'No Due Date'
            end as DueDateStatus,
            [IssueId],
            [UserId],
            cast([Description] as nvarchar(4000)) Description,
            [TenantVendorId],
            coalesce([RootIssueActionId], [Id]) RootIssueActionId,
            [Deprecated],
            IdRef,
            RegisterItemTaskId IdRef_New,
            [Status],
            case
                when Status = 0 then 'New' when Status = 3 then 'In-Progress' when Status = 1 then 'Completed'
            end as StatusCode,
            cast(coalesce(LastModificationTime,CreationTime) as datetime2) as UpdateTime
        from {{ source("issue_models", "IssueAction") }}
        {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "IssueAction") }},
    {{ col_rename("CreationTime", "IssueAction") }},
    {{ col_rename("LastModificationTime", "IssueAction") }},
    {{ col_rename("TenantId", "IssueAction") }},
    {{ col_rename("Title", "IssueAction") }},
    {{ col_rename("DueDate", "IssueAction") }},
    {{ col_rename("CompletedDate", "IssueAction") }},
    {{ col_rename("DueDateStatus", "IssueAction") }},

    {{ col_rename("IssueId", "IssueAction") }},
    {{ col_rename("UserId", "IssueAction") }},
    {{ col_rename("Description", "IssueAction") }},
    {{ col_rename("TenantVendorId", "IssueAction") }},

    {{ col_rename("RootIssueActionId", "IssueAction") }},
    {{ col_rename("Deprecated", "IssueAction") }},
    {{ col_rename("IdRef", "IssueAction") }},
    {{ col_rename("IdRef_New", "IssueAction") }},
    {{ col_rename("Status", "IssueAction") }},
    {{ col_rename("StatusCode", "IssueAction") }},
    {{ col_rename("UpdateTime", "IssueAction") }}
from
    base

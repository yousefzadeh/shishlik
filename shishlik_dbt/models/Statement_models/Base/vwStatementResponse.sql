{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [StatementDueDate],
            cast([Response] as nvarchar(4000)) Response,
            [StatementId],
            [UserId],
            [Status],
            case
                when [Status] = 0
                then 'New'
                when [Status] = 1
                then 'Completed'
                else 'In-Progress'
            end as StatusCode,
            [IsDeprecated],
            [CompletedDate],
            case
                -- Response by User is the latest
                when
                    row_number() over (
                        partition by StatementId, UserId
                        order by StatementId, UserId, Id desc
                    )
                    = 1
                then 1
                else 0
            end StatementUserIsCurrent,
            [DeletedForAssigneeRemoved]
        from
            {{ source("statement_models", "StatementResponse") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "StatementResponse") }},
    {{ col_rename("CreationTime", "StatementResponse") }},
    {{ col_rename("LastModificationTime", "StatementResponse") }},
    {{ col_rename("TenantId", "StatementResponse") }},
    {{ col_rename("StatementDueDate", "StatementResponse") }},
    {{ col_rename("Response", "StatementResponse") }},

    {{ col_rename("StatementId", "StatementResponse") }},
    {{ col_rename("UserId", "StatementResponse") }},
    {{ col_rename("Status", "StatementResponse") }},
    {{ col_rename("StatusCode", "StatementResponse") }},
    {{ col_rename("IsDeprecated", "StatementResponse") }},
    {{ col_rename("CompletedDate", "StatementResponse") }},
    {{ col_rename("StatementUserIsCurrent", "StatementResponse") }},
    {{ col_rename("DeletedForAssigneeRemoved", "StatementResponse") }}
from base

with
    base as (
        select
            -- system generated fieldsfrom app macro
            [Id],
            [CreationTime],
            [CreatorUserId],
            [LastModificationTime],
            [LastModifierUserId],
            [IsDeleted],
            [DeleterUserId],
            [DeletionTime],
            [TenantId],
            cast([Comment] as nvarchar(4000))[Comment],
            [UserId],
            [IssueActionId],
            coalesce([RootIssueActionId], [Id]) RootIssueActionId,
            [ActivityType],
            [FieldName],
            case
                when [FieldName] = 1
                then 'New'
                when [FieldName] = 2
                then 'Description'
                when [FieldName] = 3
                then 'Due Date'
                when [FieldName] = 4
                then 'User'
                when [FieldName] = 5
                then 'Status'
                when [FieldName] = 6
                then 'Third Party / Vendor'
                when [FieldName] = 7
                then 'Document'
                else 'Undefined'
            end as [FieldNameDescription],
            [IsUserActivity],
            [NewDateFieldValue],
            cast([NewFieldValue] as nvarchar(4000)) NewFieldValue,
            cast(
                COALESCE([NewFieldValue], CONVERT(VARCHAR, [NewDateFieldValue], 120), 'Empty') as nvarchar(4000)
            ) as NewValue,
            [OldDateFieldValue],
            cast([OldFieldValue] as nvarchar(4000)) OldFieldValue,
            cast(
                COALESCE([OldFieldValue], CONVERT(VARCHAR, [OldDateFieldValue], 120), 'Empty') as nvarchar(4000)
            ) as OldValue,
            [IsDateField],
            [IsRichTextField]

        from {{ source("issue_models", "IssueActionComment") }}
        {{ system_remove_IsDeleted() }}
    )
select
    {{ col_rename("Id", "IssueActionComment") }},
    {{ col_rename("TenantId", "IssueActionComment") }},
    {{ col_rename("Comment", "IssueActionComment") }},
    {{ col_rename("UserId", "IssueActionComment") }},
    {{ col_rename("CreationTime", "IssueActionComment") }},
    {{ col_rename("CreatorUserId", "IssueActionComment") }},

    {{ col_rename("IssueActionId", "IssueActionComment") }},
    {{ col_rename("RootIssueActionId", "IssueActionComment") }},
    {{ col_rename("ActivityType", "IssueActionComment") }},
    {{ col_rename("FieldName", "IssueActionComment") }},
    {{ col_rename("FieldNameDescription", "IssueActionComment") }},

    {{ col_rename("IsUserActivity", "IssueActionComment") }},
    {{ col_rename("NewDateFieldValue", "IssueActionComment") }},
    {{ col_rename("NewFieldValue", "IssueActionComment") }},
    {{ col_rename("NewValue", "IssueActionComment") }},
    {{ col_rename("OldDateFieldValue", "IssueActionComment") }},

    {{ col_rename("OldFieldValue", "IssueActionComment") }},
    {{ col_rename("OldValue", "IssueActionComment") }},
    {{ col_rename("IsDateField", "IssueActionComment") }},
    {{ col_rename("IsRichTextField", "IssueActionComment") }}
from
    base

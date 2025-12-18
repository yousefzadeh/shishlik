with
    base as (
        select
            Id,
            TenantId,
            [Name],
            EntityType,
            CreationTime,
            CreatorUserId,
            LastModificationTime,
            LastModifierUserId,
            IsDeleted,
            DeleterUserId,
            DeletionTime
        from {{ source("workflow_models", "Workflow") }} r {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Workflow") }},
    {{ col_rename("TenantId", "Workflow") }},
    {{ col_rename("Name", "Workflow") }},
    {{ col_rename("EntityType", "Workflow") }},

    {{ col_rename("CreationTime", "Workflow") }},
    {{ col_rename("CreatorUserId", "Workflow") }},  -- derived   
    {{ col_rename("LastModificationTime", "Workflow") }},
    {{ col_rename("LastModifierUserId", "Workflow") }},

    {{ col_rename("IsDeleted", "Workflow") }},
    {{ col_rename("DeleterUserId", "Workflow") }},
    {{ col_rename("DeletionTime", "Workflow") }}
from base

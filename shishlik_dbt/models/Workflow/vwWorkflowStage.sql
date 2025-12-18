with
    base as (
        select
            Id,
            TenantId,
            [Name],
            Description,
            [Order],
            IsDefault,
            TransitionFromAll,
            TransitionToAll,
            WorkflowId,
            CreationTime,
            CreatorUserId,
            LastModificationTime,
            LastModifierUserId,
            IsDeleted,
            DeleterUserId,
            DeletionTime
        from {{ source("workflow_models", "WorkflowStage") }} r {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "WorkflowStage") }},
    {{ col_rename("TenantId", "WorkflowStage") }},
    {{ col_rename("Name", "WorkflowStage") }},
    {{ col_rename("Description", "WorkflowStage") }},

    {{ col_rename("Order", "WorkflowStage") }},
    {{ col_rename("IsDefault", "WorkflowStage") }},
    {{ col_rename("TransitionFromAll", "WorkflowStage") }},
    {{ col_rename("TransitionToAll", "WorkflowStage") }},
    {{ col_rename("WorkflowId", "WorkflowStage") }},

    {{ col_rename("CreationTime", "WorkflowStage") }},
    {{ col_rename("CreatorUserId", "WorkflowStage") }},  -- derived   
    {{ col_rename("LastModificationTime", "WorkflowStage") }},
    {{ col_rename("LastModifierUserId", "WorkflowStage") }},

    {{ col_rename("IsDeleted", "WorkflowStage") }},
    {{ col_rename("DeleterUserId", "WorkflowStage") }},
    {{ col_rename("DeletionTime", "WorkflowStage") }}
from base

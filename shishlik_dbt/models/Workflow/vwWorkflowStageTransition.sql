with
    base as (
        select
            Id,
            TenantId,
            FromWorkflowStageId,
            ToWorkflowStageId,
            CreationTime,
            CreatorUserId,
            LastModificationTime,
            LastModifierUserId,
            IsDeleted,
            DeleterUserId,
            DeletionTime
        from {{ source("workflow_models", "WorkflowStageTransition") }} r {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "WorkflowStageTransition") }},
    {{ col_rename("TenantId", "WorkflowStageTransition") }},
    {{ col_rename("FromWorkflowStageId", "WorkflowStageTransition") }},
    {{ col_rename("ToWorkflowStageId", "WorkflowStageTransition") }},

    {{ col_rename("CreationTime", "WorkflowStageTransition") }},
    {{ col_rename("CreatorUserId", "WorkflowStageTransition") }},  -- derived   
    {{ col_rename("LastModificationTime", "WorkflowStageTransition") }},
    {{ col_rename("LastModifierUserId", "WorkflowStageTransition") }},

    {{ col_rename("IsDeleted", "WorkflowStageTransition") }},
    {{ col_rename("DeleterUserId", "WorkflowStageTransition") }},
    {{ col_rename("DeletionTime", "WorkflowStageTransition") }}
from base

{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [AssessmentId],
            [UserId],
            cast([FullProofCode] as nvarchar(4000)) FullProofCode,
            [IsVerified],
            cast([PartialProofCode] as nvarchar(4000)) PartialProofCode,
            [Action],
            [NewStatus],
            [OldStatus],
            cast([ChainpointHash] as nvarchar(4000)) ChainpointHash,
            cast([ChainpointJSON] as nvarchar(4000)) ChainpointJSON,
            cast([ChainpointHashNodeId] as nvarchar(4000)) ChainpointHashNodeId,
            cast([ChainpointGetFullProofProofResponseJSON] as nvarchar(4000)) ChainpointGetFullProofProofResponseJSON,
            cast([ChainpointGetPartialProofResponseJSON] as nvarchar(4000)) ChainpointGetPartialProofResponseJSON,
            cast([ChainpointSubmitHashResponseJSON] as nvarchar(4000)) ChainpointSubmitHashResponseJSON,
            cast([ChainpointVerifyFullProofResponseJSON] as nvarchar(4000)) ChainpointVerifyFullProofResponseJSON,
            cast([ChainpointVerifyPartialProofResponseJSON] as nvarchar(4000)) ChainpointVerifyPartialProofResponseJSON,
            cast([Comments] as nvarchar(4000)) Comments,
            [RootAssessmentId]
        from {{ source("AuditTrail_models", "AuditTrail") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AuditTrail") }},
    {{ col_rename("CreationTime", "AuditTrail") }},
    {{ col_rename("CreatorUserId", "AuditTrail") }},
    {{ col_rename("TenantId", "AuditTrail") }},

    {{ col_rename("AssessmentId", "AuditTrail") }},
    {{ col_rename("UserId", "AuditTrail") }},
    {{ col_rename("FullProofCode", "AuditTrail") }},
    {{ col_rename("IsVerified", "AuditTrail") }},

    {{ col_rename("PartialProofCode", "AuditTrail") }},
    {{ col_rename("Action", "AuditTrail") }},
    {{ col_rename("NewStatus", "AuditTrail") }},
    {{ col_rename("OldStatus", "AuditTrail") }},

    {{ col_rename("ChainpointHash", "AuditTrail") }},
    {{ col_rename("ChainpointJSON", "AuditTrail") }},
    {{ col_rename("ChainpointHashNodeId", "AuditTrail") }},
    {{ col_rename("ChainpointGetFullProofProofResponseJSON", "AuditTrail") }},

    {{ col_rename("ChainpointGetPartialProofResponseJSON", "AuditTrail") }},
    {{ col_rename("ChainpointSubmitHashResponseJSON", "AuditTrail") }},
    {{ col_rename("ChainpointVerifyFullProofResponseJSON", "AuditTrail") }},
    {{ col_rename("ChainpointVerifyPartialProofResponseJSON", "AuditTrail") }},

    {{ col_rename("Comments", "AuditTrail") }},
    {{ col_rename("RootAssessmentId", "AuditTrail") }}
from base

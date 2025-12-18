with
    base as (
        select
            Id,
            CreationTime,
            CreatorUserId,
            LastModificationTime,
            LastModifierUserId,
            IsDeleted,
            DeleterUserId,
            DeletionTime,
            TenantId,
            [Comment],
            UserId,
            RiskTreatmentPlanId
        from {{ source("risk_models", "RiskTreatmentPlanComment") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskTreatmentPlanComment") }},
    {{ col_rename("CreationTime", "RiskTreatmentPlanComment") }},
    {{ col_rename("CreatorUserId", "RiskTreatmentPlanComment") }},
    {{ col_rename("LastModificationTime", "RiskTreatmentPlanComment") }},

    {{ col_rename("LastModifierUserId", "RiskTreatmentPlanComment") }},
    {{ col_rename("IsDeleted", "RiskTreatmentPlanComment") }},
    {{ col_rename("DeleterUserId", "RiskTreatmentPlanComment") }},
    {{ col_rename("DeletionTime", "RiskTreatmentPlanComment") }},

    {{ col_rename("TenantId", "RiskTreatmentPlanComment") }},
    {{ col_rename("Comment", "RiskTreatmentPlanComment") }},
    {{ col_rename("UserId", "RiskTreatmentPlanComment") }},
    {{ col_rename("RiskTreatmentPlanId", "RiskTreatmentPlanComment") }}
from base

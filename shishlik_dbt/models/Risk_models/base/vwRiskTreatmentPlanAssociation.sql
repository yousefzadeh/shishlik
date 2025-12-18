{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [RiskTreatmentPlanId], [RiskTreatmentId], [RiskId]
        from {{ source("risk_models", "RiskTreatmentPlanAssociation") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskTreatmentPlanAssociation") }},
    {{ col_rename("CreationTime", "RiskTreatmentPlanAssociation") }},
    {{ col_rename("CreatorUserId", "RiskTreatmentPlanAssociation") }},
    {{ col_rename("LastModificationTime", "RiskTreatmentPlanAssociation") }},

    {{ col_rename("LastModifierUserId", "RiskTreatmentPlanAssociation") }},
    {{ col_rename("IsDeleted", "RiskTreatmentPlanAssociation") }},
    {{ col_rename("DeleterUserId", "RiskTreatmentPlanAssociation") }},
    {{ col_rename("DeletionTime", "RiskTreatmentPlanAssociation") }},

    {{ col_rename("TenantId", "RiskTreatmentPlanAssociation") }},
    {{ col_rename("RiskTreatmentPlanId", "RiskTreatmentPlanAssociation") }},
    {{ col_rename("RiskTreatmentId", "RiskTreatmentPlanAssociation") }},
    {{ col_rename("RiskId", "RiskTreatmentPlanAssociation") }}
from base

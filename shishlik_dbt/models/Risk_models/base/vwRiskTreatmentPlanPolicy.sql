{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [PolicyId], [RiskTreatmentPlanId], [TenantId]
        from {{ source("risk_models", "RiskTreatmentPlanPolicy") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskTreatmentPlanPolicy") }},
    {{ col_rename("CreationTime", "RiskTreatmentPlanPolicy") }},
    {{ col_rename("CreatorUserId", "RiskTreatmentPlanPolicy") }},
    {{ col_rename("LastModificationTime", "RiskTreatmentPlanPolicy") }},

    {{ col_rename("LastModifierUserId", "RiskTreatmentPlanPolicy") }},
    {{ col_rename("IsDeleted", "RiskTreatmentPlanPolicy") }},
    {{ col_rename("DeleterUserId", "RiskTreatmentPlanPolicy") }},
    {{ col_rename("DeletionTime", "RiskTreatmentPlanPolicy") }},

    {{ col_rename("PolicyId", "RiskTreatmentPlanPolicy") }},
    {{ col_rename("RiskTreatmentPlanId", "RiskTreatmentPlanPolicy") }},
    {{ col_rename("TenantId", "RiskTreatmentPlanPolicy") }}
from base

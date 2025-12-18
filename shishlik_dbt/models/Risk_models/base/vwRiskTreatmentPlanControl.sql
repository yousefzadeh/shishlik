{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [ControlId], [RiskTreatmentPlanId], [TenantId]
        from {{ source("risk_models", "RiskTreatmentPlanControl") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskTreatmentPlanControl") }},
    {{ col_rename("CreationTime", "RiskTreatmentPlanControl") }},
    {{ col_rename("CreatorUserId", "RiskTreatmentPlanControl") }},
    {{ col_rename("LastModificationTime", "RiskTreatmentPlanControl") }},

    {{ col_rename("LastModifierUserId", "RiskTreatmentPlanControl") }},
    {{ col_rename("IsDeleted", "RiskTreatmentPlanControl") }},
    {{ col_rename("DeleterUserId", "RiskTreatmentPlanControl") }},
    {{ col_rename("DeletionTime", "RiskTreatmentPlanControl") }},

    {{ col_rename("ControlId", "RiskTreatmentPlanControl") }},
    {{ col_rename("RiskTreatmentPlanId", "RiskTreatmentPlanControl") }},
    {{ col_rename("TenantId", "RiskTreatmentPlanControl") }}
from base

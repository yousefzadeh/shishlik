{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [AuthorityProvisionId], [RiskTreatmentPlanId]
        from {{ source("risk_models", "RiskTreatmentPlanProvision") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskTreatmentPlanProvision") }},
    {{ col_rename("CreationTime", "RiskTreatmentPlanProvision") }},
    {{ col_rename("CreatorUserId", "RiskTreatmentPlanProvision") }},
    {{ col_rename("LastModificationTime", "RiskTreatmentPlanProvision") }},

    {{ col_rename("LastModifierUserId", "RiskTreatmentPlanProvision") }},
    {{ col_rename("IsDeleted", "RiskTreatmentPlanProvision") }},
    {{ col_rename("DeleterUserId", "RiskTreatmentPlanProvision") }},
    {{ col_rename("DeletionTime", "RiskTreatmentPlanProvision") }},

    {{ col_rename("TenantId", "RiskTreatmentPlanProvision") }},
    {{ col_rename("AuthorityProvisionId", "RiskTreatmentPlanProvision") }},
    {{ col_rename("RiskTreatmentPlanId", "RiskTreatmentPlanProvision") }}
from base

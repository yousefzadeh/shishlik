{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [RiskTreatmentPlanId], [UserId], [OrganizationUnitId]
        from {{ source("risk_models", "RiskTreatmentPlanOwner") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskTreatmentPlanOwner") }},
    {{ col_rename("CreationTime", "RiskTreatmentPlanOwner") }},
    {{ col_rename("CreatorUserId", "RiskTreatmentPlanOwner") }},
    {{ col_rename("LastModificationTime", "RiskTreatmentPlanOwner") }},

    {{ col_rename("LastModifierUserId", "RiskTreatmentPlanOwner") }},
    {{ col_rename("IsDeleted", "RiskTreatmentPlanOwner") }},
    {{ col_rename("DeleterUserId", "RiskTreatmentPlanOwner") }},
    {{ col_rename("DeletionTime", "RiskTreatmentPlanOwner") }},

    {{ col_rename("TenantId", "RiskTreatmentPlanOwner") }},
    {{ col_rename("RiskTreatmentPlanId", "RiskTreatmentPlanOwner") }},
    {{ col_rename("UserId", "RiskTreatmentPlanOwner") }},
    {{ col_rename("OrganizationUnitId", "RiskTreatmentPlanOwner") }},
    u.AbpUsers_FullName RiskTreatmentPlanOwner_FullName,
    u.AbpUsers_UserName RiskTreatmentPlanOwner_UserName,
    o.AbpOrganizationUnits_DisplayName RiskTreatmentPlanOwner_OrganisationName

from base
left join {{ ref("vwAbpUser") }} u on base.UserId = u.AbpUsers_Id
left join {{ ref("vwAbpOrganizationUnits") }} o on base.OrganizationUnitId = o.AbpOrganizationUnits_Id

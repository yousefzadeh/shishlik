{{ config(materialized="view") }}
-- Risk Owner in filter is a union of RiskOwner user and organisation
with
    base as (select * from {{ ref("vwRiskTreatmentPlanOwner") }}),
    [user] as (
        select
            RiskTreatmentPlanOwner_TenantId,
            RiskTreatmentPlanOwner_RiskTreatmentPlanId,
            RiskTreatmentPlanOwner_FullName OwnerText
        from base
        where RiskTreatmentPlanOwner_FullName is not null
    ),
    org as (
        select
            RiskTreatmentPlanOwner_TenantId,
            RiskTreatmentPlanOwner_RiskTreatmentPlanId,
            RiskTreatmentPlanOwner_OrganisationName OwnerText
        from base
        where RiskTreatmentPlanOwner_OrganisationName is not null
    )
select *
from [user]
union all
select *
from org

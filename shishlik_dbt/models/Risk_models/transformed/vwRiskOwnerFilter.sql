{{ config(materialized="view") }}
-- Risk Owner in filter is a union of RiskOwner user and organisation
with
    base as (select * from {{ ref("vwRiskOwner") }}),
    [user] as (
        select RiskOwner_TenantId, RiskOwner_RiskId, RiskOwner_UserId as Owner_Id, RiskOwner_FullName OwnerText
        from base
        where RiskOwner_FullName is not null
    ),
    org as (
        select RiskOwner_TenantId, RiskOwner_RiskId, RiskOwner_OrganizationUnitId as Owner_Id, RiskOwner_OrganisationName OwnerText
        from base
        where RiskOwner_OrganisationName is not null
    )
select *
from [user]
union all
select *
from org

{{ config(materialized="view") }}
-- Risk User in filter is a union of RiskUser user and organisation
with
    base as (select * from {{ ref("vwRiskUser") }}),
    [user] as (
        select RiskUser_TenantId, RiskUser_RiskId, RiskUser_UserId as AccessMember_Id, RiskUser_FullName UserText
        from base
        where RiskUser_FullName is not null
    ),
    org as (
        select RiskUser_TenantId, RiskUser_RiskId, RiskUser_OrganizationUnitId as AccessMember_Id, RiskUser_OrganisationName OwnerText
        from base
        where RiskUser_OrganisationName is not null
    )
select *
from [user]
union all
select *
from org

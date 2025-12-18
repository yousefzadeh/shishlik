{{ config(materialized="view") }}
with
    base as (select * from {{ ref("vwIssueOwner") }}),
    [user] as (select IssueOwner_TenantId, IssueOwner_IssueId, IssueOwner_UserId Owner_Id, IssueOwner_FullName OwnerName from base),
    org as (select IssueOwner_TenantId, IssueOwner_IssueId, IssueOwner_OrganizationUnitId Owner_Id, IssueOwner_OrganisationName OwnerName from base)
select *
from [user]
union all
select *
from org

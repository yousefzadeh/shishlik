{{ config(materialized="view") }}
with
    base as (select * from {{ ref("vwIssueUser") }}),
    [user] as (select IssueUser_TenantId, IssueUser_IssueId, IssueUser_UserId Member_Id, IssueUser_FullName UserText from base),
    org as (select IssueUser_TenantId, IssueUser_IssueId, IssueUser_OrganizationUnitId Member_Id, IssueUser_OrganisationName UserText from base)
select *
from [user]
union all
select *
from org

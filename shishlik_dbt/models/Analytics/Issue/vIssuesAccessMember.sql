with acs_memb as (
select
iu.Uuid,
iu.TenantId,
iu.IssueId,
iu.UserId AccessMemberId,
au.Name+' '+au.Surname Issues_AccessMember
from {{ source("issue_ref_models", "IssueUser") }} iu
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = iu.UserId and au.IsDeleted = 0
where iu.IsDeleted = 0

union all

select
iu.Uuid,
iu.TenantId,
iu.IssueId,
iu.OrganizationUnitId AccessMemberId,
aou.DisplayName Issues_AccessMember
from {{ source("issue_ref_models", "IssueUser") }} iu
join {{ source("abp_ref_models", "AbpOrganizationUnits") }} aou
on aou.Id = iu.OrganizationUnitId and aou.IsDeleted = 0
where iu.IsDeleted = 0
)

select
Uuid,
TenantId,
IssueId Issues_Id,
AccessMemberId Issues_AccessMemberId,
Issues_AccessMember
from acs_memb
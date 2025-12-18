with owner as (
select
io.Uuid,
io.TenantId,
io.IssueId,
io.UserId OwnerId,
au.Name+' '+au.Surname Issues_OwnerName
from {{ source("issue_ref_models", "IssueOwner") }} io
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = io.UserId and au.IsDeleted = 0
where io.IsDeleted = 0

union all

select
io.Uuid,
io.TenantId,
io.IssueId,
io.OrganizationUnitId OwnerId,
aou.DisplayName Issues_OwnerName
from {{ source("issue_ref_models", "IssueOwner") }} io
join {{ source("abp_ref_models", "AbpOrganizationUnits") }} aou
on aou.Id = io.OrganizationUnitId and aou.IsDeleted = 0
where io.IsDeleted = 0
)

select
Uuid,
TenantId,
IssueId Issues_Id,
OwnerId Issues_OwnerId,
Issues_OwnerName
from owner
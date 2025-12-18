with owner as (
select
io.Uuid,
io.TenantId,
io.IssueId,
io.UserId OwnerId,
au.Name+' '+au.Surname Asset_OwnerName
from {{ source("asset_ref_models", "IssueOwner") }} io
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = io.UserId and au.IsDeleted = 0
where io.IsDeleted = 0

union all

select
io.Uuid,
io.TenantId,
io.IssueId,
io.OrganizationUnitId OwnerId,
aou.DisplayName Asset_OwnerName
from {{ source("asset_ref_models", "IssueOwner") }} io
join {{ source("abp_ref_models", "AbpOrganizationUnits") }} aou
on aou.Id = io.OrganizationUnitId and aou.IsDeleted = 0
where io.IsDeleted = 0
)

select
Uuid,
TenantId,
IssueId Asset_Id,
OwnerId Asset_OwnerId,
Asset_OwnerName
from owner
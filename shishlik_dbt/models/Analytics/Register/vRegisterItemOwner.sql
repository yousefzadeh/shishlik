with owner as (
select
io.Uuid,
io.TenantId,
io.IssueId,
io.UserId OwnerId,
au.Name+' '+au.Surname RegisterItem_OwnerName
from {{ source("register_ref_models", "IssueOwner") }} io
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = io.UserId and au.IsDeleted = 0
where io.IsDeleted = 0

union all

select
io.Uuid,
io.TenantId,
io.IssueId,
io.OrganizationUnitId OwnerId,
aou.DisplayName RegisterItem_OwnerName
from {{ source("register_ref_models", "IssueOwner") }} io
join {{ source("abp_ref_models", "AbpOrganizationUnits") }} aou
on aou.Id = io.OrganizationUnitId and aou.IsDeleted = 0
where io.IsDeleted = 0
)

select
Uuid,
TenantId,
IssueId RegisterItem_Id,
OwnerId RegisterItem_OwnerId,
RegisterItem_OwnerName
from owner
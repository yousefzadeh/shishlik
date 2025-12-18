with owner as (
select
ro.Uuid,
ro.TenantId,
ro.RiskId,
ro.UserId OwnerId,
au.Name+' '+au.Surname Risk_OwnerName
from {{ source("risk_ref_models", "RiskOwner") }} ro
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = ro.UserId and au.IsDeleted = 0
where ro.IsDeleted = 0

union all

select
ro.Uuid,
ro.TenantId,
ro.RiskId,
ro.OrganizationUnitId OwnerId,
aou.DisplayName Risk_OwnerName
from {{ source("risk_ref_models", "RiskOwner") }} ro
join {{ source("abp_ref_models", "AbpOrganizationUnits") }} aou
on aou.Id = ro.OrganizationUnitId and aou.IsDeleted = 0
where ro.IsDeleted = 0
)

select
Uuid,
TenantId,
RiskId Risk_Id,
OwnerId Risk_OwnerId,
Risk_OwnerName
from owner
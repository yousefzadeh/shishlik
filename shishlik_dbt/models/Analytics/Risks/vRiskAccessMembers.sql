with acs_memb as (
select
ru.Uuid,
ru.TenantId,
ru.RiskId,
ru.UserId AccessMemberId,
au.Id UserId,
au.Name+' '+au.Surname Risk_AccessMember
from {{ source("risk_ref_models", "RiskUser") }} ru
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = ru.UserId and au.IsDeleted = 0
where ru.IsDeleted = 0

union all

select
ru.Uuid,
ru.TenantId,
ru.RiskId,
ru.OrganizationUnitId AccessMemberId,
au.Id UserId,
aou.DisplayName Risk_AccessMember
from {{ source("risk_ref_models", "RiskUser") }} ru
join {{ source("abp_ref_models", "AbpOrganizationUnits") }} aou
on aou.Id = ru.OrganizationUnitId and aou.IsDeleted = 0
join {{ source("abp_ref_models", "AbpUserOrganizationUnits") }} auou
on auou.OrganizationUnitId = aou.Id and auou.IsDeleted = 0
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = auou.UserId and au.IsDeleted = 0 and au.IsActive = 1
where ru.IsDeleted = 0
)

select
Uuid,
TenantId,
RiskId Risk_Id,
AccessMemberId Risk_AccessMemberId,
UserId,
Risk_AccessMember
from acs_memb
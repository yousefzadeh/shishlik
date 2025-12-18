with asgn as (
select
rtpo.Uuid,
rtpo.TenantId,
rtpo.Id,
rtpo.RiskTreatmentPlanId,
rtpo.UserId AssigneeId,
au.Name+' '+au.Surname RiskTreatmentPlan_AssigneeName

from {{ source("risk_ref_models", "RiskTreatmentPlanOwner") }} rtpo
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = rtpo.UserId and au.IsDeleted = 0 and au.IsActive = 1
where rtpo.IsDeleted = 0

union all

select
rtpo.Uuid,
rtpo.TenantId,
rtpo.Id,
rtpo.RiskTreatmentPlanId,
rtpo.OrganizationUnitId AssigneeId,
aou.DisplayName RiskTreatmentPlan_AssigneeName

from {{ source("risk_ref_models", "RiskTreatmentPlanOwner") }} rtpo
join {{ source("abp_ref_models", "AbpOrganizationUnits") }} aou
on aou.Id = rtpo.OrganizationUnitId and aou.IsDeleted = 0
where rtpo.IsDeleted = 0
)

select
Uuid,
TenantId,
Id RiskTreatmentPlanAssignee_Id,
RiskTreatmentPlanId RiskTreatmentPlan_Id,
AssigneeId RiskTreatmentPlan_AssigneeId,
RiskTreatmentPlan_AssigneeName
from asgn
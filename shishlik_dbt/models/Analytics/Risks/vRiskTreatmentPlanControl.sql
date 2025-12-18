select
rtpc.TenantId,
rtpc.RiskTreatmentPlanId RiskTreatmentPlan_Id,
rtpc.ControlId RiskTreatmentPlan_ControlId,
c.Name RiskTreatmentPlan_ControlName

from {{ source("risk_ref_models", "RiskTreatmentPlanControl") }} rtpc
join {{ source("controlset_ref_models", "Controls") }} c
on c.Id = rtpc.ControlId and c.IsDeleted = 0
where rtpc.IsDeleted = 0
select
rtpp.TenantId,
rtpp.RiskTreatmentPlanId RiskTreatmentPlan_Id,
rtpp.PolicyId RiskTreatmentPlan_PolicyId,
p.Name RiskTreatmentPlan_PolicyName

from {{ source("risk_ref_models", "RiskTreatmentPlanPolicy") }} rtpp
join {{ source("controlset_ref_models", "Policy") }} p
on p.Id = rtpp.PolicyId and p.IsDeleted = 0
where rtpp.IsDeleted = 0
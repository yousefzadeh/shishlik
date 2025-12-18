select
rtpp.TenantId,
ap.Authority_Id,
rtpp.AuthorityProvisionId AuthorityProvision_Id,
rtpp.RiskTreatmentPlanId RiskTreatmentPlan_Id,
rtp.RiskTreatmentPlan_Name AuthorityProvision_LinkedTreatmentPlans

from {{ source("risk_ref_models", "RiskTreatmentPlanProvision") }} rtpp
join {{ ref("vAuthorityProvision") }} ap
on ap.AuthorityProvision_Id = rtpp.AuthorityProvisionId
join {{ ref("vRiskTreatmentPlan") }} rtp on rtp.RiskTreatmentPlan_Id = rtpp.RiskTreatmentPlanId
where rtpp.IsDeleted = 0
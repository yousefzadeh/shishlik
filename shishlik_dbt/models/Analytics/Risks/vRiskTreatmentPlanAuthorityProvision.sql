select
rtpp.TenantId,
rtpp.RiskTreatmentPlanId RiskTreatmentPlan_Id,
rtpp.AuthorityProvisionId RiskTreatmentPlan_AuthorityProvisionId,
ap.Name RiskTreatmentPlan_AuthorityProvisionName,
a.Name RiskTreatmentPlan_AuthorityName

from {{ source("risk_ref_models", "RiskTreatmentPlanProvision") }} rtpp
join {{ source("authority_ref_models", "AuthorityProvision") }} ap
on ap.Id = rtpp.AuthorityProvisionId and ap.IsDeleted = 0
join {{ source("authority_ref_models", "Authority") }} a
on a.Id = ap.AuthorityId and a.IsDeleted = 0
where rtpp.IsDeleted = 0
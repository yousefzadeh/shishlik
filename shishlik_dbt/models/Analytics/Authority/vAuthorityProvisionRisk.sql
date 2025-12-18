select
rp.TenantId,
ap.Authority_Id,
rp.AuthorityProvisionId AuthorityProvision_Id,
rp.RiskId Risk_Id,
r.Risk_Name AuthorityProvision_linkedRisks

from {{ source("risk_ref_models", "RiskProvision") }} rp
join {{ ref("vAuthorityProvision") }} ap
on ap.AuthorityProvision_Id = rp.AuthorityProvisionId
join {{ ref("vRisks") }} r on r.Risk_Id = rp.RiskId
and rp.TenantId = r.TenantId
where rp.IsDeleted = 0
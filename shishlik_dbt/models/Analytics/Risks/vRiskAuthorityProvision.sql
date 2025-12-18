select
rp.TenantId,
rp.RiskId Risk_Id,
rp.AuthorityProvisionId Risk_AuthorityProvisionId,
ap.Name Risk_AuthorityProvisionName,
a.Name Risk_AuthorityName

from {{ source("risk_ref_models", "RiskProvision") }} rp
join {{ source("authority_ref_models", "AuthorityProvision") }} ap
on ap.Id = rp.AuthorityProvisionId and ap.IsDeleted = 0
join {{ source("authority_ref_models", "Authority") }} a
on a.Id = ap.AuthorityId and a.IsDeleted = 0
where rp.IsDeleted = 0
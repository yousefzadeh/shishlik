select
rp.TenantId,
rp.RiskId Risk_Id,
rp.PolicyId Risk_PolicyId,
p.Name Risk_PolicyName

from {{ source("risk_ref_models", "RiskPolicy") }} rp
join {{ source("controlset_ref_models", "Policy") }} p
on p.Id = rp.PolicyId and p.IsDeleted = 0
where rp.IsDeleted = 0
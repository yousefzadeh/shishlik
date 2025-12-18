select
rc.TenantId,
rc.RiskId Risk_Id,
rc.ControlId Risk_ControlId,
c.Name Risk_ControlName

from {{ source("risk_ref_models", "RiskControl") }} rc
join {{ source("controlset_ref_models", "Controls") }} c
on c.Id = rc.ControlId and c.IsDeleted = 0
where rc.IsDeleted = 0
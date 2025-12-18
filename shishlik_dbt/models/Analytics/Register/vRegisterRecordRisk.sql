select distinct
ir.TenantId,
ir.IssueId RegisterRecord_Id,
ir.RiskId RegisterRecord_LinkedRiskId,
r.Name RegisterRecord_LinkedRisk

from {{ source("register_ref_models", "IssueRisk") }} ir
join {{ source("risk_ref_models", "Risk") }} r
on r.TenantId = ir.TenantId
and r.Id = ir.RiskId and r.IsDeleted = 0
where ir.IsDeleted = 0
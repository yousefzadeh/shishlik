select
ir.TenantId,
ir.IssueId Issues_Id,
ir.RiskId Issues_LinkedRiskId,
r.Name Issues_LinkedRisk

from {{ source("issue_ref_models", "IssueRisk") }} ir
join {{ source("risk_ref_models", "Risk") }} r
on r.Id = ir.RiskId and r.IsDeleted = 0
where ir.IsDeleted = 0
select
ir.TenantId,
ir.IssueId Asset_Id,
ir.RiskId Asset_LinkedRiskId,
r.Name Asset_LinkedRisk

from {{ source("asset_ref_models", "IssueRisk") }} ir
join {{ source("risk_ref_models", "Risk") }} r
on r.Id = ir.RiskId and r.IsDeleted = 0
where ir.IsDeleted = 0
select 
ir.TenantId,
ir.RiskId Risk_Id,
ir.IssueId Risk_AssetId,
i.Name Risk_LinkedAsset

from {{ source("issue_ref_models", "IssueRisk") }} ir
join {{ source("issue_ref_models", "Issues") }} i on i.Id = ir.IssueId and i.IsDeleted = 0
join {{ source("issue_ref_models", "EntityRegister") }} er on er.Id = i.EntityRegisterId and er.EntityType = 5
where ir.IsDeleted = 0
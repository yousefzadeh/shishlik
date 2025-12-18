select 
ir.TenantId,
ir.RiskId Risk_Id,
ir.IssueId Risk_IssueId,
i.Name Risk_LinkedIssue

from {{ source("register_ref_models", "IssueRisk") }} ir
join {{ source("register_ref_models", "Issues") }} i
on i.Id = ir.IssueId and i.IsDeleted = 0 and i.IsArchived = 0
join {{ source("register_ref_models", "EntityRegister") }} er
on er.Id = i.EntityRegisterId and er.IsDeleted = 0 and er.EntityType = 3
where ir.IsDeleted = 0
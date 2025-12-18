select distinct
ir.TenantId,
ir.RiskId Risk_Id,
er.Id Risk_CustomRegisterId,
er.Name Risk_LinkedCustomRegister

from {{ source("register_ref_models", "IssueRisk") }} ir
join {{ source("register_ref_models", "Issues") }} i
on i.Id = ir.IssueId and i.IsDeleted = 0 and i.IsArchived = 0
join {{ source("register_ref_models", "EntityRegister") }} er
on er.Id = i.EntityRegisterId and er.IsDeleted = 0 and er.EntityType = 4
where ir.IsDeleted = 0
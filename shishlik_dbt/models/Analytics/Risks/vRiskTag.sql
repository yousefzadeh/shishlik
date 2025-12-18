select
rt.Uuid,
rt.TenantId,
rt.RiskId Risk_Id,
rt.TagId Risk_TagId,
t.Name Risk_Tag
from {{ source("risk_ref_models", "RiskTag") }} rt
join {{ source("miscellaneous_ref_models", "Tags") }} t
on t.Id = rt.TagId and t.IsDeleted = 0
where rt.IsDeleted = 0
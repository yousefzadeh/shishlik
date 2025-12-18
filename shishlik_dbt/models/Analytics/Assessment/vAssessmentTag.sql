select
atg.Uuid,
atg.TenantId,
atg.AssessmentId Assessment_Id,
atg.TagId Assessment_TagId,
t.Name Assessment_Tag
from {{ source("assessment_ref_models", "AssessmentTag") }} atg
join {{ source("miscellaneous_ref_models", "Tags") }} t
on t.Id = atg.TagId and t.IsDeleted = 0
where atg.IsDeleted = 0
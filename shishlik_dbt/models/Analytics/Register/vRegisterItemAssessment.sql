select 
ai.TenantId,
ai.RegisterItemId RegisterItem_Id,
ai.AssessmentId RegisterItem_AssessmentId,
a.Name RegisterItem_LinkedAssessment

from {{ source("assessment_ref_models", "AssessmentScopeRegisterItem") }} ai
join {{ source("assessment_ref_models", "Assessment") }} a
on a.TenantId = ai.TenantId
and a.Id = ai.AssessmentId and a.IsDeleted = 0
where ai.IsDeleted = 0
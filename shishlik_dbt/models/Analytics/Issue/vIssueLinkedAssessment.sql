select 
ai.TenantId,
ai.RegisterItemId Issues_Id,
ai.AssessmentId Issues_AssessmentId,
a.Name Issues_LinkedAssessment

from {{ source("assessment_ref_models", "AssessmentScopeRegisterItem") }} ai
join {{ source("assessment_ref_models", "Assessment") }} a
on a.TenantId = ai.TenantId
and a.Id = ai.AssessmentId and a.IsDeleted = 0
where ai.IsDeleted = 0
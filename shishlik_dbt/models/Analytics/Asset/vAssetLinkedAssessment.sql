select 
ai.TenantId,
ai.RegisterItemId Asset_Id,
ai.AssessmentId Asset_AssessmentId,
a.Name Asset_LinkedAssessment

from {{ source("assessment_ref_models", "AssessmentScopeRegisterItem") }} ai
join {{ source("assessment_ref_models", "Assessment") }} a
on a.TenantId = ai.TenantId
and a.Id = ai.AssessmentId and a.IsDeleted = 0
where ai.IsDeleted = 0
select 
ar.TenantId,
ar.RiskId Risk_Id,
ar.AssessmentId Risk_AssessmentId,
a.Name Risk_LinkedAssessment

from {{ source("assessment_ref_models", "AssessmentRisk") }} ar
join {{ source("assessment_ref_models", "Assessment") }} a
on a.Id = ar.AssessmentId and a.IsDeleted = 0
where ar.IsDeleted = 0
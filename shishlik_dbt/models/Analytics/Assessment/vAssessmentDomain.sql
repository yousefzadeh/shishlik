select
ad.Uuid,
ad.TenantId,
ad.AssessmentId Assessment_Id,
ad.Id AssessmentDomain_Id,
ad.[Name] AssessmentDomain_Name,
ad.[Description] AssessmentDomain_Description,
ad.[Order] AssessmentDomain_Order

from {{ source("assessment_ref_models", "AssessmentDomain") }} ad
where ad.IsDeleted = 0
select
at.TenantId,
at.AssessmentId AssessmentTag_AssessmentId,
at.TagId AssessmentTag_TagId,
t.Tags_Name Assessment_Tags

from {{ source('assessment_models', 'AssessmentTag') }} at
join {{ref('vwTags')}} t
on t.Tags_Id = at.TagId
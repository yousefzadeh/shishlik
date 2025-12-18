select
qtg.Uuid,
qtg.TenantId,
qtg.QuestionId Question_Id,
qtg.TagId Question_TagId,
t.[Name] Question_Tag

from {{ source("assessment_ref_models", "QuestionTags") }} qtg
join {{ source("miscellaneous_ref_models", "Tags") }} t
on t.Id = qtg.TagId and t.IsDeleted = 0
where qtg.IsDeleted = 0
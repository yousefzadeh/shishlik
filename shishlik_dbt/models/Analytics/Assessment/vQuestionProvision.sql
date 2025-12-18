select
pq.TenantId,
pq.Id QuestionProvision_Id,
pq.CreationTime QuestionProvision_CreationTime,
pq.QuestionId Question_Id,
pq.AuthorityProvisionId AuthorityProvision_Id

from {{ source("assessment_ref_models", "ProvisionQuestion") }} pq
where pq.IsDeleted = 0
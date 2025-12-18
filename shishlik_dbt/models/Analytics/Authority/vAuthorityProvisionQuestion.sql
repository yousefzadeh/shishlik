select
pq.TenantId,
pq.Id ProvisionQuestion_Id,
pq.AuthorityProvisionId AuthorityProvision_Id,
pq.QuestionId Question_Id,
q.Question_Name Provision_LinkedQuestions

from {{ source("assessment_ref_models", "ProvisionQuestion") }} pq
join {{ ref("vQuestion") }} q on q.Question_Id = pq.QuestionId
where pq.IsDeleted = 0
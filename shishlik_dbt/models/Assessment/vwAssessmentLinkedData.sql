with assess as(
select distinct * from {{ ref("vwUserAssessments") }} ua
)
,auth as(
select distinct
a.Assessment_Id, a.Assessment_TenantId, auth.Name LinkedAuthority, ap.Name LinkedAuthorityProvision
from assess a
left join {{ source("assessment_models", "Authority") }} auth on auth.Id = a.Assessment_AuthorityId and auth.IsDeleted = 0
join {{ source("assessment_models", "AssessmentDomain") }} ad on ad.AssessmentId = a.Assessment_Id and ad.IsDeleted = 0
left join {{ source("assessment_models", "Question") }} q on q.AssessmentDomainId = ad.Id and q.IsDeleted = 0
join {{ source("assessment_models", "ProvisionQuestion") }} pq on pq.QuestionId = q.Id and pq.IsDeleted = 0
join {{ source("assessment_models", "AuthorityProvision") }} ap on ap.Id = pq.AuthorityProvisionId and ap.IsDeleted = 0
)
, policy_cte as(
select distinct
a.Assessment_Id, a.Assessment_TenantId, p.Name LinkedPolicy, c.Name LinkedControls
from assess a
left join {{ source("assessment_models", "Policy") }} p on p.Id = a.Assessment_PolicyId and p.IsDeleted = 0
join {{ source("assessment_models", "AssessmentDomain") }} ad on ad.AssessmentId = a.Assessment_Id and ad.IsDeleted = 0
left join {{ source("assessment_models", "Question") }} q on q.AssessmentDomainId = ad.Id and q.IsDeleted = 0
join {{ source("assessment_models", "ControlQuestion") }} cq on cq.QuestionId = q.Id and cq.IsDeleted = 0
join {{ source("assessment_models", "Controls") }} c on c.Id = cq.ControlsId and c.IsDeleted = 0
)

select
a.Assessment_Id, a.Assessment_TenantId, a.TenantName, a.Assessment_Name,--a.UserName, 
auth.LinkedAuthority, auth.LinkedAuthorityProvision, p.LinkedPolicy, p.LinkedControls
from assess a
left join auth on auth.Assessment_Id = a.Assessment_Id and auth.Assessment_TenantId = a.Assessment_TenantId
left join policy_cte p on p.Assessment_Id = a.Assessment_Id and p.Assessment_TenantId = a.Assessment_TenantId
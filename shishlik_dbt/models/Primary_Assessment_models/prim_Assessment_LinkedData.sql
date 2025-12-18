with auth_policy as(
select distinct
a.TenantId,
abp.Name Tenant_Name,
tv.Name TenantVendor_Name,
a.Id Assessment_Id,
a.Name Assessment_Name,
auth.Name Assessment_LinkedAuthority,
p.Name Assessment_LinkedControlSet,
ad.Id AssessmentDomain_Id,
ad.Name AssessmentDomain_Name,
q.Id Question_Id,
q.Name Question_Name

from {{ source("assessment_models", "Assessment") }} a
join {{ source("assessment_models", "AbpTenants") }} abp on abp.Id = a.TenantId
join {{ source("tenant_models", "TenantVendor") }} tv on tv.Id = a.TenantVendorId and tv.TenantId = a.TenantId and tv.IsDeleted = 0
join {{ source("assessment_models", "AssessmentDomain") }} ad on ad.AssessmentId = a.Id and ad.IsDeleted = 0
join {{ source("assessment_models", "Question") }} q on q.AssessmentDomainId = ad.Id and q.IsDeleted = 0
left join {{ source("assessment_models", "Authority") }} auth on auth.Id = a.AuthorityId and auth.IsDeleted = 0
left join {{ source("assessment_models", "Policy") }} p on p.Id = a.PolicyId and p.IsDeleted = 0

where a.IsDeleted = 0 and a.IsTemplate = 0
and a.IsDeprecatedAssessmentVersion = 0
and a.Status != 8 and a.IsArchived = 0
and abp.IsDeleted = 0 and abp.IsActive = 1
)
, linked_prov as (
select distinct
aupo.TenantId, aupo.Assessment_Id, aupo.Question_Id, string_agg(ap.ReferenceId, ', ') Question_LinkedProvisionRefIdList, string_agg(ap.Name, ', ') Question_LinkedProvisionList
from auth_policy aupo
left join {{ source("assessment_models", "ProvisionQuestion") }} pq on pq.QuestionId = aupo.Question_Id and pq.IsDeleted = 0
left join {{ source("assessment_models", "AuthorityProvision") }} ap on ap.Id = pq.AuthorityProvisionId and ap.IsDeleted = 0
group by
aupo.TenantId,
aupo.Assessment_Id,
aupo.Question_Id
)
, linked_ctrl as (
select distinct
aupo.TenantId, aupo.Assessment_Id, aupo.Question_Id, string_agg(c.Reference, ', ') Question_LinkedControlRefIdList, string_agg(c.Name, ', ') Question_LinkedControlList
from auth_policy aupo
left join {{ source("assessment_models", "ControlQuestion") }} cq on cq.QuestionId = aupo.Question_Id and cq.IsDeleted = 0
left join {{ source("assessment_models", "Controls") }} c on c.Id = cq.ControlsId and c.IsDeleted = 0
group by
aupo.TenantId,
aupo.Assessment_Id,
aupo.Question_Id
)
, linked_risks as (
select distinct
ap.TenantId, ap.Assessment_Id, ap.Question_Id, string_agg(r.Name, ', ') Question_LinkedRiskList
from auth_policy ap
left join {{ source("assessment_models", "AssessmentRisk") }} ar on ar.AssessmentId = ap.Assessment_Id and ar.TenantId = ap.TenantId and ar.IsDeleted = 0
left join {{ source("risk_models", "Risk") }} r on r.Id = ar.RiskId and r.TenantId = ar.TenantId  and r.IsDeleted = 0
group by
ap.TenantId,
ap.Assessment_Id,
ap.Question_Id
)
, linked_issues as (
select distinct
ap.TenantId, ap.Assessment_Id, ap.Question_Id, string_agg(i.Name, ', ') Question_LinkedIssueList
from auth_policy ap
left join {{ source("issue_models", "IssueAssessment") }} ia on ia.AssessmentId = ap.Assessment_Id and ia.TenantId = ap.TenantId and ia.IsDeleted = 0 
left join {{ source("issue_models", "Issues") }} i on i.Id = ia.IssueId and i.TenantId = ia.TenantId and i.IsDeleted = 0
group by
ap.TenantId,
ap.Assessment_Id,
ap.Question_Id
)

select distinct
ap.TenantId,
ap.Tenant_Name,
ap.TenantVendor_Name,
ap.Assessment_Id,
ap.Assessment_Name,
ap.Assessment_LinkedAuthority,
lp.Question_LinkedProvisionRefIdList,
lp.Question_LinkedProvisionList,
ap.Assessment_LinkedControlSet,
lc.Question_LinkedControlRefIdList,
lc.Question_LinkedControlList,
ap.AssessmentDomain_Id,
ap.AssessmentDomain_Name,
ap.Question_Id,
ap.Question_Name,
lr.Question_LinkedRiskList,
li.Question_LinkedIssueList
from auth_policy ap
left join linked_prov lp
on lp.TenantId = ap.TenantId
and lp.Assessment_Id = ap.Assessment_Id
and lp.Question_Id = ap.Question_Id
left join linked_ctrl lc
on lc.TenantId = ap.TenantId
and lc.Assessment_Id = ap.Assessment_Id
and lc.Question_Id = ap.Question_Id
left join linked_risks lr 
on lr.TenantId = ap.TenantId
and lr.Assessment_Id = ap.Assessment_Id
and lr.Question_Id = ap.Question_Id
left join linked_issues li
on li.TenantId = ap.TenantId
and li.Assessment_Id = ap.Assessment_Id
and li.Question_Id = ap.Question_Id
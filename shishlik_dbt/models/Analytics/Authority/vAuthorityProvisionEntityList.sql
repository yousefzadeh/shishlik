with prov as (
select
ap.Authority_Id,
ap.AuthorityProvision_Id,
ap.AuthorityProvision_IdRef,
ap.AuthorityProvision_Name
from {{ref("vAuthorityProvision")}} ap
)
, prov_ques as (
select
apq.TenantId, apq.AuthorityProvision_Id, string_agg(apq.Provision_LinkedQuestions , '; ') Provision_QuestionList
from {{ref("vAuthorityProvisionQuestion")}} apq
group by apq.TenantId, apq.AuthorityProvision_Id
)
, prov_ctrl as (
select
apc.TenantId, apc.AuthorityProvision_Id, string_agg(apc.Provision_LinkedControls, '; ') Provision_ControlList
from {{ref("vAuthorityProvisionControl")}} apc
group by apc.TenantId, apc.AuthorityProvision_Id
)
, prov_cusreg as (
select
ac.TenantId, ac.AuthorityProvision_Id, string_agg(ac.Provision_linkedCustomRegisters, '; ') Provision_CustomRegisterList
from {{ref("vAuthorityProvisionCustomRegister")}} ac
group by ac.TenantId, ac.AuthorityProvision_Id
)
, prov_iss as (
select
ai.TenantId, ai.AuthorityProvision_Id, string_agg(ai.Provision_linkedIssues, '; ') Provision_IssueList
from {{ref("vAuthorityProvisionIssue")}} ai
group by ai.TenantId, ai.AuthorityProvision_Id
)
, prov_ris as (
select
ar.TenantId, ar.AuthorityProvision_Id, string_agg(ar.AuthorityProvision_linkedRisks, '; ') Provision_RiskList
from {{ref("vAuthorityProvisionRisk")}} ar
group by ar.TenantId, ar.AuthorityProvision_Id
)
, prov_trtpln as (
select 
atp.TenantId, atp.AuthorityProvision_Id, string_agg(atp.AuthorityProvision_LinkedTreatmentPlans, '; ') Provision_TreatmentPlanList
from {{ref("vAuthorityProvisionTreatmentPlan")}} atp
group by atp.TenantId, atp.AuthorityProvision_Id
)

select
ap.Authority_Id,
ap.AuthorityProvision_Id,
ap.AuthorityProvision_IdRef,
ap.AuthorityProvision_Name,
pq.Provision_QuestionList,
pc.Provision_ControlList,
pcr.Provision_CustomRegisterList,
pi.Provision_IssueList,
pr.Provision_RiskList,
pt.Provision_TreatmentPlanList

from prov ap
left join prov_ques pq on pq.AuthorityProvision_Id = ap.AuthorityProvision_Id
left join prov_ctrl pc on pc.AuthorityProvision_Id = ap.AuthorityProvision_Id
left join prov_cusreg pcr on pcr.AuthorityProvision_Id = ap.AuthorityProvision_Id
left join prov_iss pi on pi.AuthorityProvision_Id = ap.AuthorityProvision_Id
left join prov_ris pr on pr.AuthorityProvision_Id = ap.AuthorityProvision_Id
left join prov_trtpln pt on pt.AuthorityProvision_Id = ap.AuthorityProvision_Id
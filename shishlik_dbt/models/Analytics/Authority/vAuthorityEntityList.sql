with auth as (
select
a.TenantId,
a.Authority_Id,
a.Authority_Name
from {{ref("vAuthority")}} a
)
, auth_assess as (
select
aa.TenantId, aa.Authority_Id, string_agg(aa.Authority_LinkedAssessments , '; ') Authority_AssessmentList
from {{ref("vAuthorityAssessment")}} aa
group by aa.TenantId, aa.Authority_Id
)
, auth_ctrlset as (
select
acs.TenantId, acs.Authority_Id, string_agg(acs.Authority_LinkedControlSets , '; ') Authority_ControlSetList
from {{ref("vAuthorityControlSet")}} acs
group by acs.TenantId, acs.Authority_Id
)
, auth_cusreg as (
select distinct
acr.TenantId, acr.Authority_Id,acr.Provision_linkedCustomRegisters
from {{ref("vAuthorityProvisionCustomRegister")}} acr
)
, auth_cusreglis as (
select
ac.TenantId, ac.Authority_Id, string_agg(ac.Provision_linkedCustomRegisters, '; ') Authority_CustomRegisterList
from auth_cusreg ac
group by ac.TenantId, ac.Authority_Id
)
, auth_iss as (
select distinct
ai.TenantId, ai.Authority_Id,ai.Provision_linkedIssues
from {{ref("vAuthorityProvisionIssue")}} ai
)
, auth_isslis as (
select
ai.TenantId, ai.Authority_Id, string_agg(ai.Provision_linkedIssues, '; ') Authority_IssueList
from auth_iss ai
group by ai.TenantId, ai.Authority_Id
)
, auth_ris as (
select distinct
ar.TenantId, ar.Authority_Id, ar.AuthorityProvision_linkedRisks
from {{ref("vAuthorityProvisionRisk")}} ar
)
, auth_rislist as (
select distinct
ar.TenantId, ar.Authority_Id, string_agg(ar.AuthorityProvision_linkedRisks, '; ') Authority_RiskList
from auth_ris ar
group by ar.TenantId, ar.Authority_Id
)
, auth_trtpln as (
select distinct
atp.TenantId, atp.Authority_Id, atp.AuthorityProvision_LinkedTreatmentPlans
from {{ref("vAuthorityProvisionTreatmentPlan")}} atp
)
, auth_trtplnlist as (
select distinct
atp.TenantId, atp.Authority_Id, string_agg(atp.AuthorityProvision_LinkedTreatmentPlans, '; ') Authority_TreatmentPlanList
from auth_trtpln atp
group by atp.TenantId, atp.Authority_Id
)

select
a.TenantId,
a.Authority_Id,
a.Authority_Name,
aa.Authority_AssessmentList,
ac.Authority_ControlSetList,
acr.Authority_CustomRegisterList,
ai.Authority_IssueList,
ar.Authority_RiskList,
atp.Authority_TreatmentPlanList

from auth a
left join auth_assess aa on aa.Authority_Id = a.Authority_Id and aa.TenantId = a.TenantId
left join auth_ctrlset ac on ac.Authority_Id = a.Authority_Id and ac.TenantId = a.TenantId
left join auth_cusreglis acr on acr.Authority_Id = a.Authority_Id and acr.TenantId = a.TenantId
left join auth_isslis ai on ai.Authority_Id = a.Authority_Id and ai.TenantId = a.TenantId
left join auth_rislist ar on ar.Authority_Id = a.Authority_Id and ar.TenantId = a.TenantId
left join auth_trtplnlist atp on atp.Authority_Id = a.Authority_Id and atp.TenantId = a.TenantId
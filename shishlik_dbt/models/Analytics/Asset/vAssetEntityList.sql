with ast as (
select
a.TenantId,
a.TenantName,
a.Asset_Id,
a.Asset_Name
from {{ ref("vAsset") }} a
)
, proj as (
select
ap.TenantId, ap.Asset_Id, string_agg(ap.Asset_LinkedProject, '; ') Asset_LinkedProjectList
from {{ ref("vAssetProject") }} ap
group by
ap.TenantId, ap.Asset_Id
)
, proj_tsk as (
select
ap.TenantId, ap.Asset_Id, string_agg(ap.Asset_LinkedProjectTask, '; ') Asset_LinkedProjectTaskList
from {{ ref("vAssetProjectTask") }} ap
group by
ap.TenantId, ap.Asset_Id
)
, asses as (
select
aa.TenantId, aa.Asset_Id, string_agg(aa.Asset_LinkedAssessment, '; ') Asset_AssessmentList
from {{ ref("vAssetLinkedAssessment") }} aa
group by
aa.TenantId, aa.Asset_Id
)
, iss as (
select
ia.TenantId, ia.Asset_Id, string_agg(ia.Asset_LinkedIssue, '; ') Asset_IssueList
from {{ ref("vAssetIssue") }} ia
group by
ia.TenantId, ia.Asset_Id
)
, cus_regis as (
select
acr.TenantId, acr.Asset_Id, string_agg(acr.Asset_LinkedRegister, '; ') Asset_CustomRegisterList
from {{ ref("vAssetCustomRegister") }} acr
group by
acr.TenantId, acr.Asset_Id
)
, cus_regis_itm as (
select
acri.TenantId, acri.Asset_Id, string_agg(acri.Asset_LinkedRegisterItem, '; ') Asset_CustomRegisterItemList
from {{ ref("vAssetCustomRegisterItem") }} acri
group by
acri.TenantId, acri.Asset_Id
)
, ris as (
select
ar.TenantId, ar.Asset_Id, string_agg(ar.Asset_LinkedRisk, '; ') Asset_RiskList
from {{ ref("vAssetRisk") }} ar
group by
ar.TenantId, ar.Asset_Id
)
, cset as (
select
ac.TenantId, ac.Asset_Id, string_agg(ac.Asset_LinkedControlSet, '; ') Asset_ControlSetList
from {{ ref("vAssetControlSet") }} ac
group by
ac.TenantId, ac.Asset_Id
)
, ctrl as (
select
ac.TenantId, ac.Asset_Id, string_agg(ac.Asset_LinkedControl, '; ') Asset_ControlList
from {{ ref("vAssetControl") }} ac
group by
ac.TenantId, ac.Asset_Id
)
, ctrl_resp as (
select
ac.TenantId, ac.Asset_Id, string_agg(ac.Asset_LinkedControlResponsibility, '; ') Asset_ControlResponsibilityList
from {{ ref("vAssetControlResponsibility") }} ac
group by
ac.TenantId, ac.Asset_Id
)
, third_pty as (
select
atp.TenantId, atp.Asset_Id, string_agg(atp.Asset_LinkedThirdParty, '; ') Asset_ThirdPartyList
from {{ ref("vAssetThirdParty") }} atp
group by
atp.TenantId, atp.Asset_Id
)
, auth as (
select
aa.TenantId, aa.Asset_Id, string_agg(aa.Asset_LinkedAuthority, '; ') Asset_AuthorityList
from {{ ref("vAssetAuthority") }} aa
group by
aa.TenantId, aa.Asset_Id
)
, auth_prov as (
select
ap.TenantId, ap.Asset_Id, string_agg(ap.Asset_LinkedProvision, '; ') Asset_ProvisionList
from {{ ref("vAssetProvision") }} ap
group by
ap.TenantId, ap.Asset_Id
)

select
a.TenantId,
a.TenantName,
a.Asset_Id,
a.Asset_Name,
p.Asset_LinkedProjectList,
pt.Asset_LinkedProjectTaskList,
aa.Asset_AssessmentList,
cr.Asset_CustomRegisterList,
cri.Asset_CustomRegisterItemList,
i.Asset_IssueList,
r.Asset_RiskList,
cs.Asset_ControlSetList,
c.Asset_ControlList,
crs.Asset_ControlResponsibilityList,
tp.Asset_ThirdPartyList,
au.Asset_AuthorityList,
ap.Asset_ProvisionList

from ast a
left join proj p on p.TenantId = a.TenantId and p.Asset_Id = a.Asset_Id
left join proj_tsk pt on pt.TenantId = a.TenantId and pt.Asset_Id = a.Asset_Id
left join asses aa on aa.TenantId = a.TenantId and aa.Asset_Id = a.Asset_Id
left join iss i on i.TenantId = a.TenantId and i.Asset_Id = a.Asset_Id
left join cus_regis cr on cr.TenantId = a.TenantId and cr.Asset_Id = a.Asset_Id
left join cus_regis_itm cri on cri.TenantId = a.TenantId and cri.Asset_Id = a.Asset_Id
left join ris r on r.TenantId = a.TenantId and r.Asset_Id = a.Asset_Id
left join cset cs on cs.TenantId = a.TenantId and cs.Asset_Id = a.Asset_Id
left join ctrl c on c.TenantId = a.TenantId and c.Asset_Id = a.Asset_Id
left join ctrl_resp crs on crs.TenantId = a.TenantId and crs.Asset_Id = a.Asset_Id
left join third_pty tp on tp.TenantId = a.TenantId and tp.Asset_Id = a.Asset_Id
left join auth au on au.TenantId = a.TenantId and au.Asset_Id = a.Asset_Id
left join auth_prov ap on ap.TenantId = a.TenantId and ap.Asset_Id = a.Asset_Id
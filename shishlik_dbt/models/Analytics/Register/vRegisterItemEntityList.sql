with reg_itm as (
select
rr.TenantId,
rr.TenantName,
rr.Record_Id RegisterItem_Id,
rr.Record_Name RegisterItem_Name
from {{ ref("vRegisterRecord") }} rr
)
, proj as (
select
rip.TenantId, rip.RegisterItem_Id, string_agg(rip.RegisterItem_LinkedProject, '; ') RegisterItem_LinkedProjectList
from {{ ref("vRegisterItemProject") }} rip
group by
rip.TenantId, rip.RegisterItem_Id
)
, proj_tsk as (
select
rpt.TenantId, rpt.RegisterItem_Id, string_agg(rpt.RegisterItem_LinkedProjectTask, '; ') RegisterItem_LinkedProjectTaskList
from {{ ref("vRegisterItemProjectTask") }} rpt
group by
rpt.TenantId, rpt.RegisterItem_Id
)
, ast as (
select
ra.TenantId, ra.RegisterItem_Id, string_agg(ra.RegisterItem_LinkedAsset, '; ') RegisterItem_AssetList
from {{ ref("vRegisterItemAsset") }} ra
group by
ra.TenantId, ra.RegisterItem_Id
)
, asses as (
select
ra.TenantId, ra.RegisterItem_Id, string_agg(ra.RegisterItem_LinkedAssessment, '; ') RegisterItem_LinkedAssessmentList
from {{ ref("vRegisterItemAssessment") }} ra
group by
ra.TenantId, ra.RegisterItem_Id
)
, iss as (
select
ri.TenantId, ri.RegisterItem_Id, string_agg(ri.RegisterItem_LinkedIssue, '; ') RegisterItem_IssueList
from {{ ref("vRegisterItemIssue") }} ri
group by
ri.TenantId, ri.RegisterItem_Id
)
, cus_regis as (
select
r.TenantId, r.RegisterItem_Id, string_agg(r.RegisterItem_LinkedRegister, '; ') RegisterItem_CustomRegisterList
from {{ ref("vRegisterItemLinkedRegister") }} r
group by
r.TenantId, r.RegisterItem_Id
)
, cus_regis_itm as (
select
r.TenantId, r.RegisterItem_Id, string_agg(r.RegisterItem_LinkedRegisterItem, '; ') RegisterItem_CustomRegisterItemList
from {{ ref("vRegisterItemLinkedRegisterItem") }} r
group by
r.TenantId, r.RegisterItem_Id
)
, ris as (
select
r.TenantId, r.RegisterRecord_Id, string_agg(r.RegisterRecord_LinkedRisk, '; ') RegisterItem_RiskList
from {{ ref("vRegisterRecordRisk") }} r
group by
r.TenantId, r.RegisterRecord_Id
)
, cset as (
select
rc.TenantId, rc.RegisterItem_Id, string_agg(rc.RegisterItem_LinkedControlSet, '; ') RegisterItem_ControlSetList
from {{ ref("vRegisterItemControlSet") }} rc
group by
rc.TenantId, rc.RegisterItem_Id
)
, ctrl as (
select
rc.TenantId, rc.RegisterItem_Id, string_agg(rc.RegisterItem_LinkedControl, '; ') RegisterItem_ControlList
from {{ ref("vRegisterItemControl") }} rc
group by
rc.TenantId, rc.RegisterItem_Id
)
, ctrl_resp as (
select
rc.TenantId, rc.RegisterItem_Id, string_agg(rc.RegisterItem_LinkedControlResponsibility, '; ') RegisterItem_ControlResponsibilityList
from {{ ref("vRegisterItemControlResponsibility") }} rc
group by
rc.TenantId, rc.RegisterItem_Id
)
, third_pty as (
select
rtp.TenantId, rtp.RegisterItem_Id, string_agg(rtp.RegisterItem_LinkedThirdParty, '; ') RegisterItem_ThirdPartyList
from {{ ref("vRegisterItemThirdParty") }} rtp
group by
rtp.TenantId, rtp.RegisterItem_Id
)
, auth as (
select
ra.TenantId, ra.RegisterItem_Id, string_agg(ra.RegisterItem_LinkedAuthority, '; ') RegisterItem_AuthorityList
from {{ ref("vRegisterItemAuthority") }} ra
group by
ra.TenantId, ra.RegisterItem_Id
)
, auth_prov as (
select
rp.TenantId, rp.RegisterRecord_Id, string_agg(rp.RegisterItem_LinkedProvision, '; ') RegisterItem_ProvisionList
from {{ ref("vRegisterRecordProvision") }} rp
group by
rp.TenantId, rp.RegisterRecord_Id
)

select
ri.TenantId,
ri.TenantName,
ri.RegisterItem_Id,
ri.RegisterItem_Name,
p.RegisterItem_LinkedProjectList,
pt.RegisterItem_LinkedProjectTaskList,
ast.RegisterItem_AssetList,
aa.RegisterItem_LinkedAssessmentList,
cr.RegisterItem_CustomRegisterList,
cri.RegisterItem_CustomRegisterItemList,
i.RegisterItem_IssueList,
r.RegisterItem_RiskList,
cs.RegisterItem_ControlSetList,
c.RegisterItem_ControlList,
crs.RegisterItem_ControlResponsibilityList,
tp.RegisterItem_ThirdPartyList,
au.RegisterItem_AuthorityList,
ap.RegisterItem_ProvisionList

from reg_itm ri
left join proj p on p.TenantId = ri.TenantId and p.RegisterItem_Id = ri.RegisterItem_Id
left join proj_tsk pt on pt.TenantId = ri.TenantId and pt.RegisterItem_Id = ri.RegisterItem_Id
left join ast on ast.TenantId = ri.TenantId and ast.RegisterItem_Id = ri.RegisterItem_Id
left join asses aa on aa.TenantId = ri.TenantId and aa.RegisterItem_Id = ri.RegisterItem_Id
left join iss i on i.TenantId = ri.TenantId and i.RegisterItem_Id = ri.RegisterItem_Id
left join cus_regis cr on cr.TenantId = ri.TenantId and cr.RegisterItem_Id = ri.RegisterItem_Id
left join cus_regis_itm cri on cri.TenantId = ri.TenantId and cri.RegisterItem_Id = ri.RegisterItem_Id
left join ris r on r.TenantId = ri.TenantId and r.RegisterRecord_Id = ri.RegisterItem_Id
left join cset cs on cs.TenantId = ri.TenantId and cs.RegisterItem_Id = ri.RegisterItem_Id
left join ctrl c on c.TenantId = ri.TenantId and c.RegisterItem_Id = ri.RegisterItem_Id
left join ctrl_resp crs on crs.TenantId = ri.TenantId and crs.RegisterItem_Id = ri.RegisterItem_Id
left join third_pty tp on tp.TenantId = ri.TenantId and tp.RegisterItem_Id = ri.RegisterItem_Id
left join auth au on au.TenantId = ri.TenantId and au.RegisterItem_Id = ri.RegisterItem_Id
left join auth_prov ap on ap.TenantId = ri.TenantId and ap.RegisterRecord_Id = ri.RegisterItem_Id
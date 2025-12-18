with iss as (
select
i.TenantId,
i.TenantName,
i.Issues_Id,
i.Issues_Name
from {{ ref("vIssues") }} i
)
, proj as (
select
ip.TenantId, ip.Issues_Id, string_agg(ip.Issues_LinkedProject, '; ') Issue_LinkedProjectList
from {{ ref("vIssueProject") }} ip
group by
ip.TenantId, ip.Issues_Id
)
, proj_tsk as (
select
ip.TenantId, ip.Issues_Id, string_agg(ip.Issues_LinkedProjectTask, '; ') Issue_LinkedProjectTaskList
from {{ ref("vIssueProjectTask") }} ip
group by
ip.TenantId, ip.Issues_Id
)
, asses as (
select
ia.TenantId, ia.Issues_Id, string_agg(ia.Issues_LinkedAssessment, '; ') Issue_AssessmentList
from {{ ref("vIssueLinkedAssessment") }} ia
group by
ia.TenantId, ia.Issues_Id
)
, asset as (
select
ia.TenantId, ia.Issues_Id, string_agg(ia.Issues_LinkedAsset, '; ') Issue_AssetList
from {{ ref("vIssueAsset") }} ia
group by
ia.TenantId, ia.Issues_Id
)
, cus_regis as (
select
icr.TenantId, icr.Issues_Id, string_agg(icr.Issues_LinkedRegister, '; ') Issue_CustomRegisterList
from {{ ref("vIssueCustomRegister") }} icr
group by
icr.TenantId, icr.Issues_Id
)
, cus_regis_itm as (
select
icri.TenantId, icri.Issues_Id, string_agg(icri.Issues_LinkedRegisterItem, '; ') Issue_CustomRegisterItemList
from {{ ref("vIssueCustomRegisterItem") }} icri
group by
icri.TenantId, icri.Issues_Id
)
, ris as (
select
ir.TenantId, ir.Issues_Id, string_agg(ir.Issues_LinkedRisk, '; ') Issue_RiskList
from {{ ref("vIssueRisk") }} ir
group by
ir.TenantId, ir.Issues_Id
)
, cset as (
select
ic.TenantId, ic.Issues_Id, string_agg(ic.Issues_LinkedControlSet, '; ') Issue_ControlSetList
from {{ ref("vIssueControlSet") }} ic
group by
ic.TenantId, ic.Issues_Id
)
, ctrl as (
select
ic.TenantId, ic.Issues_Id, string_agg(ic.Issues_LinkedControl, '; ') Issue_ControlList
from {{ ref("vIssueControl") }} ic
group by
ic.TenantId, ic.Issues_Id
)
, ctrl_resp as (
select
ic.TenantId, ic.Issues_Id, string_agg(ic.Issues_LinkedControlResponsibility, '; ') Issue_ControlResponsibilityList
from {{ ref("vIssueControlResponsibility") }} ic
group by
ic.TenantId, ic.Issues_Id
)
, third_pty as (
select
itp.TenantId, itp.Issues_Id, string_agg(itp.Issues_LinkedThirdParty, '; ') Issue_ThirdPartyList
from {{ ref("vIssueThirdParty") }} itp
group by
itp.TenantId, itp.Issues_Id
)
, auth as (
select
ia.TenantId, ia.Issues_Id, string_agg(ia.Issues_LinkedAuthority, '; ') Issue_AuthorityList
from {{ ref("vIssueAuthority") }} ia
group by
ia.TenantId, ia.Issues_Id
)
, auth_prov as (
select
ip.TenantId, ip.Issues_Id, string_agg(ip.Issues_LinkedProvision, '; ') Issue_ProvisionList
from {{ ref("vIssueProvision") }} ip
group by
ip.TenantId, ip.Issues_Id
)

select
i.TenantId,
i.TenantName,
i.Issues_Id,
i.Issues_Name,
p.Issue_LinkedProjectList,
pt.Issue_LinkedProjectTaskList,
aa.Issue_AssessmentList,
a.Issue_AssetList,
cr.Issue_CustomRegisterList,
cri.Issue_CustomRegisterItemList,
r.Issue_RiskList,
cs.Issue_ControlSetList,
c.Issue_ControlList,
crs.Issue_ControlResponsibilityList,
tp.Issue_ThirdPartyList,
au.Issue_AuthorityList,
ap.Issue_ProvisionList

from iss i
left join proj p on p.Issues_Id = i.Issues_Id
left join proj_tsk pt on pt.Issues_Id = i.Issues_Id
left join asses aa on aa.Issues_Id = i.Issues_Id
left join asset a on a.Issues_Id = i.Issues_Id
left join cus_regis cr on cr.Issues_Id = i.Issues_Id
left join cus_regis_itm cri on cri.Issues_Id = i.Issues_Id
left join ris r on r.Issues_Id = i.Issues_Id
left join cset cs on cs.Issues_Id = i.Issues_Id
left join ctrl c on c.Issues_Id = i.Issues_Id
left join ctrl_resp crs on crs.Issues_Id = i.Issues_Id
left join third_pty tp on tp.Issues_Id = i.Issues_Id
left join auth au on au.Issues_Id = i.Issues_Id
left join auth_prov ap on ap.Issues_Id = i.Issues_Id
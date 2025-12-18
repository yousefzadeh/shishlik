with ris as (
select
r.TenantId,
r.TenantName,
r.Risk_Id,
r.Risk_Name
from {{ ref("vRisks") }} r
)
, asset as (
select
ra.TenantId, ra.Risk_Id, string_agg(ra.Risk_LinkedAsset, '; ') Risk_AssetList
from {{ ref("vRiskAsset") }} ra
group by
ra.TenantId, ra.Risk_Id
)
, cus_regis as (
select
rcr.TenantId, rcr.Risk_Id, string_agg(rcr.Risk_LinkedCustomRegister, '; ') Risk_CustomRegisterList
from {{ ref("vRiskCustomRegister") }} rcr
group by
rcr.TenantId, rcr.Risk_Id
)
, cus_regis_itm as (
select
rcr.TenantId, rcr.Risk_Id, string_agg(rcr.Risk_LinkedCustomRegisterItem, '; ') Risk_CustomRegisterItemList
from {{ ref("vRiskCustomRegisterItem") }} rcr
group by
rcr.TenantId, rcr.Risk_Id
)
, third_pty as (
select
rtp.TenantId, rtp.Risk_Id, string_agg(rtp.Risk_LinkedThirdParty, '; ') Risk_ThirdPartyList
from {{ ref("vRiskThirdParty") }} rtp
group by
rtp.TenantId, rtp.Risk_Id
)
, assess as (
select
rla.TenantId, rla.Risk_Id, string_agg(rla.Risk_LinkedAssessment, '; ') Risk_AssessmentList
from {{ ref("vRiskLinkedAssessment") }} rla
group by
rla.TenantId, rla.Risk_Id
)
, iss as (
select
ri.TenantId, ri.Risk_Id, string_agg(ri.Risk_LinkedIssue, '; ') Risk_IssueList
from {{ ref("vRiskIssue") }} ri
group by
ri.TenantId, ri.Risk_Id
)
, ris_parent as (
select
rr.TenantId, rr.Risk_Id, string_agg(cast(rr.Risk_LinkedRiskIdRef as varchar(8)) +': '+ rr.Risk_LinkedRiskName, '; ') Risk_ParentList
from {{ ref("vRiskRelationships") }} rr
where rr.Risk_RelationshipTypeId = 1
group by
rr.TenantId, rr.Risk_Id
)
, ris_child as (
select
rr.TenantId, rr.Risk_Id, string_agg(cast(rr.Risk_LinkedRiskIdRef as varchar(8)) +': '+ rr.Risk_LinkedRiskName, '; ') Risk_ChildList
from {{ ref("vRiskRelationships") }} rr
where rr.Risk_RelationshipTypeId = 2
group by
rr.TenantId, rr.Risk_Id
)
, ris_rel as (
select
rr.TenantId, rr.Risk_Id, string_agg(cast(rr.Risk_LinkedRiskIdRef as varchar(8)) +': '+ rr.Risk_LinkedRiskName, '; ') Risk_RelatedList
from {{ ref("vRiskRelationships") }} rr
where rr.Risk_RelationshipTypeId = 3
group by
rr.TenantId, rr.Risk_Id
)
, ctrl_set as (
select
rp.TenantId, rp.Risk_Id, string_agg(rp.Risk_PolicyName, '; ') Risk_ControlSetList
from {{ ref("vRiskPolicy") }} rp
group by
rp.TenantId, rp.Risk_Id
)
, ctrl as (
select
rc.TenantId, rc.Risk_Id, string_agg(rc.Risk_ControlName, '; ') Risk_ControlList
from {{ ref("vRiskControl") }} rc
group by
rc.TenantId, rc.Risk_Id
)
, auth_prov as (
select
rap.TenantId, rap.Risk_Id,
string_agg(rap.Risk_AuthorityName, '; ') Risk_AuthorityList,
string_agg(rap.Risk_AuthorityProvisionName, '; ') Risk_AuthorityProvisionList
from {{ ref("vRiskAuthorityProvision") }} rap
group by
rap.TenantId, rap.Risk_Id
)

select
r.TenantId,
r.TenantName,
r.Risk_Id,
r.Risk_Name,
a.Risk_AssetList,
cr.Risk_CustomRegisterList,
cri.Risk_CustomRegisterItemList,
tp.Risk_ThirdPartyList,
las.Risk_AssessmentList,
rp.Risk_ParentList,
rc.Risk_ChildList,
rr.Risk_RelatedList,
i.Risk_IssueList,
cs.Risk_ControlSetList,
c.Risk_ControlList,
ap.Risk_AuthorityList,
ap.Risk_AuthorityProvisionList

from ris r
left join asset a on a.Risk_Id = r.Risk_Id
left join cus_regis cr on cr.Risk_Id = r.Risk_Id
left join cus_regis_itm cri on cri.Risk_Id = r.Risk_Id
left join third_pty tp on tp.Risk_Id = r.Risk_Id
left join assess las on las.Risk_Id = r.Risk_Id
left join iss i on i.Risk_Id = r.Risk_Id
left join ris_parent rp on rp.Risk_Id = r.Risk_Id
left join ris_child rc on rc.Risk_Id = r.Risk_Id
left join ris_rel rr on rr.Risk_Id = r.Risk_Id
left join ctrl_set cs on cs.Risk_Id = r.Risk_Id
left join ctrl c on c.Risk_Id = r.Risk_Id
left join auth_prov ap on ap.Risk_Id = r.Risk_Id
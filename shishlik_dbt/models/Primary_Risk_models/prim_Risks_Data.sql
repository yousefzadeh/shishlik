with Risks as(
select
r.TenantId,
abp.Name Tenant_Name,
r.Id Risk_Id,
cast(r.IsDeleted as int) IsDeleted,
r.TenantEntityUniqueId Risk_IdRef,
r.Name Risk_Name,
r.[Description] Risk_Description,
r.CreationTime Risk_CreationTime,
au2.Name+' '+au2.Surname Risk_CreatedBy,
COALESCE(r.LastModificationTime, r.CreationTime) Risk_LastUpdatedTime,
au3.Name+' '+au3.Surname Risk_LastUpdatedBy,
wfs.Name Risk_Status,
r.CommonCause Risk_CommonCause,
r.LikelyImpact Risk_LikelyImpact,
au.Name+' '+au.Surname Risk_IdentifiedBy,
sl.Name Risk_TreatmentDecision,
case
when r.TreatmentStatusId = 1
then 'Draft'
when r.TreatmentStatusId = 2
then 'Approved'
when r.TreatmentStatusId = 3
then 'Treatment in progress'
when r.TreatmentStatusId = 4
then 'Treatment paused'
when r.TreatmentStatusId = 5
then 'Treatment cancelled'
when r.TreatmentStatusId = 6
then 'Treatment completed'
when r.TreatmentStatusId = 7
then 'Closed'
else 'Undefined'
end as Risk_TreatmentStatus,
tpa.Label Risk_Rating


from {{ source("risk_models", "Risk") }} r
join {{ source("assessment_models", "AbpTenants") }} abp on abp.Id = r.TenantId
left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = r.IdentifiedBy and au.IsActive = 1 and au.IsDeleted = 0
left join {{ source("assessment_models", "AbpUsers") }} au2 on au2.Id = r.CreatorUserId and au2.IsActive = 1 and au2.IsDeleted = 0
left join {{ source("assessment_models", "AbpUsers") }} au3 on au3.Id = r.CreatorUserId and au3.IsActive = 1 and au3.IsDeleted = 0
join {{ source("workflow_models", "WorkflowStage") }} wfs on wfs.Id = r.WorkflowStageId and wfs.IsDeleted = 0
join {{ source("statuslists_models", "StatusLists") }} sl on sl.Id = r.TreatmentDecisionId and sl.IsDeleted = 0
left join {{ source("issue_models", "ThirdPartyAttributes") }} tpa on tpa.Id = r.RiskRatingId

where abp.IsDeleted = 0 and abp.IsActive = 1
and r.[Status] != 100
)
, Owners as (
select rof.RiskOwner_TenantId, rof.RiskOwner_RiskId, STRING_AGG(CONVERT(NVARCHAR(max), rof.OwnerText), ', ') Risk_OwnerList
from {{ ref("vwRiskOwnerFilter") }} rof
group by
rof.RiskOwner_TenantId, rof.RiskOwner_RiskId
)
,  AccessMembers as (
select ruf.RiskUser_TenantId, ruf.RiskUser_RiskId, STRING_AGG(CONVERT(NVARCHAR(max), ruf.UserText), ', ') Risk_AccessMemberList
from {{ ref("vwRiskUserFilter") }} ruf
group by
ruf.RiskUser_TenantId, ruf.RiskUser_RiskId
)
, Risk_Tags as (
select rt.TenantId, rt.RiskId, STRING_AGG(CONVERT(NVARCHAR(max), t.Name), ', ') Risk_TagsList
from {{ source("risk_models", "RiskTag") }} rt
left join {{ source("assessment_models", "Tags") }} t on t.Id = rt.TagId
and t.TenantId = rt.TenantId and t.IsDeleted = 0 and rt.IsDeleted = 0
group by
rt.TenantId, rt.RiskId
)
, domain as(
select distinct
r.TenantId,
r.Risk_Name Risk_Name,
r.Risk_Id Risk_Id,
tpc.Label Risk_Domain,
tpa.Label Risk_DomainValues,
tpa.Id ThirdPartyAttributes_Id

from Risks r
join {{ source("risk_models", "RiskCustomAttributeData") }} rcad
on rcad.RiskId = r.Risk_Id
join {{ source("issue_models", "ThirdPartyAttributes") }} tpa
on tpa.Id = rcad.ThirdPartyAttributesId
join {{ source("issue_models", "ThirdPartyControl") }} tpc
on tpc.Id = rcad.ThirdPartyControlId and tpc.Name = 'RiskDomain'
)
-- , child_domain as (
-- select 
-- d.TenantId, d.Risk_Id,
-- tpc2.Label Child_Domain,
-- tpa2.Label Child_DomainValues,
-- tpa2.Id ThirdPartyAttributes_Id2
-- from domain d
-- join RiskCustomAttributeData rcad2
-- on rcad2.RiskId = d.Risk_Id
-- left join ThirdPartyAttributes tpa2
-- on tpa2.Id = rcad2.ThirdPartyAttributesId
-- and tpa2.ParentThirdPartyAttributeId = d.ThirdPartyAttributes_Id
-- left join ThirdPartyControl tpc2
-- on tpc2.Id = tpa2.ThirdPartyControlId
-- where tpa2.Label is not null
-- )
-- , grandchild_domain as (
-- select 
-- d.TenantId, d.Risk_Id,
-- tpc3.Label Granchild_Domain,
-- tpa3.Label Granchild_DomainValues,
-- tpa3.Id ThirdPartyAttributes_Id3
-- from child_domain d
-- join {{ source("risk_models", "RiskCustomAttributeData") }} rcad3
-- on rcad3.RiskId = d.Risk_Id
-- left join {{ source("issue_models", "ThirdPartyAttributes") }} tpa3
-- on tpa3.Id = rcad3.ThirdPartyAttributesId
-- and tpa3.ParentThirdPartyAttributeId = d.ThirdPartyAttributes_Id2
-- left join {{ source("issue_models", "ThirdPartyControl") }} tpc3
-- on tpc3.Id = tpa3.ThirdPartyControlId
-- where tpa3.Label is not null
-- )
, Linked_Asset as (
SELECT r.TenantId, r.Risk_Id, STRING_AGG(CONVERT(NVARCHAR(max), a.Title), ', ') Linked_Assets
from Risks r
join {{ source("risk_models", "RiskAsset") }} ra
on ra.RiskId = r.Risk_Id and ra.TenantId = r.TenantId and ra.IsDeleted = 0
join {{ source("assessment_models", "Asset") }} a
on a.Id = ra.AssetId and a.TenantId = ra.TenantId and a.IsDeleted = 0
group by
r.TenantId, r.Risk_Id
)
, Linked_Issue as (
SELECT r.TenantId, r.Risk_Id, STRING_AGG(CONVERT(NVARCHAR(max), i.Issues_Name), ', ') Linked_Issues
from Risks r
join {{ source("issue_models", "IssueRisk") }} ir
on ir.RiskId = r.Risk_Id and ir.TenantId = r.TenantId and ir.IsDeleted = 0
join {{ ref("vwIssues") }} i
on i.Issues_Id = ir.IssueId and i.Issues_TenantId = ir.TenantId and i.Issues_Status != 100
group by
r.TenantId, r.Risk_Id
)
, Linked_Metric as (
SELECT r.TenantId, r.Risk_Id, STRING_AGG(CONVERT(NVARCHAR(max), m.Name), ', ') Linked_Metrics
from Risks r
join {{ source("risk_models", "RiskMetric") }} rm
on rm.RiskId = r.Risk_Id and rm.IsDeleted = 0
join {{ source("metric_models", "Metric") }} m
on m.Id = rm.MetricId and m.IsDeleted = 0
group by
r.TenantId, r.Risk_Id
)
, Linked_Vendor as (
SELECT r.TenantId, r.Risk_Id, STRING_AGG(CONVERT(NVARCHAR(max), tv.Name), ', ') Linked_Vendors
from Risks r
join {{ source("risk_models", "RiskThirdParty") }} rt
on rt.RiskId = r.Risk_Id and rt.TenantId = r.TenantId and rt.IsDeleted = 0
join {{ source("tenant_models", "TenantVendor") }} tv
on tv.Id= rt.TenantVendorId and tv.IsDeleted = 0
group by
r.TenantId, r.Risk_Id
)
, Linked_RegisterRecord as (
SELECT r.TenantId, r.Risk_Id, STRING_AGG(CONVERT(NVARCHAR(max), rr.Name), ', ') Linked_RegisterRecords
from Risks r
join {{ source("risk_models", "RiskRegisterRecord") }} rrr
on rrr.RiskId = r.Risk_Id and rrr.TenantId = r.TenantId and rrr.IsDeleted = 0
join {{ source("register_models", "RegisterRecord") }} rr
on rr.Id = rrr.RegisterRecordId and rr.TenantId = rrr.TenantId and rr.IsDeleted = 0
group by
r.TenantId, r.Risk_Id
)
, Linked_Assessment as (
SELECT r.TenantId, r.Risk_Id, STRING_AGG(CONVERT(NVARCHAR(max), ase.Name), ', ') Linked_Assessments
from Risks r
join {{ source("assessment_models", "AssessmentRisk") }} ar
on ar.RiskId = r.Risk_Id and ar.TenantId = r.TenantId and ar.IsDeleted = 0
join {{ source("assessment_models", "Assessment") }} ase
on ase.Id = ar.AssessmentId and ase.TenantId = ar.TenantId and ase.IsDeleted = 0
group by
r.TenantId, r.Risk_Id
)
, Linked_ControlSet as (
SELECT r.TenantId, r.Risk_Id, STRING_AGG(CONVERT(NVARCHAR(max), p.[Name]), ', ') Linked_ControlSets
from Risks r
join {{ source("risk_models", "RiskPolicy") }} rp
on rp.RiskId = r.Risk_Id and rp.TenantId = r.TenantId and rp.IsDeleted = 0
join {{ source("assessment_models", "Policy") }} p
on p.Id = rp.PolicyId and p.TenantId = rp.TenantId and p.IsDeleted = 0
group by
r.TenantId, r.Risk_Id
)
, Linked_Controls as (
SELECT r.TenantId, r.Risk_Id, STRING_AGG(CONVERT(NVARCHAR(max), c.[Name]), ', ') Linked_Controls
from Risks r
join {{ source("risk_models", "RiskControl") }} rc
on rc.RiskId = r.Risk_Id and rc.IsDeleted = 0
join {{ source("assessment_models", "Controls") }} c
on c.Id = rc.ControlId and c.IsDeleted = 0
group by
r.TenantId, r.Risk_Id
)
, Linked_Provisions as (
SELECT r.TenantId, r.Risk_Id, STRING_AGG(CONVERT(NVARCHAR(max), ap.[Name]), ', ') Linked_Provisions
from Risks r
join {{ source("risk_models", "RiskProvision") }} rp
on rp.RiskId = r.Risk_Id and rp.IsDeleted = 0
join {{ source("assessment_models", "AuthorityProvision") }} ap
on ap.Id = rp.AuthorityProvisionId and ap.IsDeleted = 0
group by
r.TenantId, r.Risk_Id
)
, distinc_Authorities as (
SELECT distinct r.TenantId, r.Risk_Id, a.[Name] Linked_Authorities
from Risks r
join {{ source("risk_models", "RiskProvision") }} rp
on rp.RiskId = r.Risk_Id and rp.IsDeleted = 0
join {{ source("assessment_models", "AuthorityProvision") }} ap
on ap.Id = rp.AuthorityProvisionId and ap.IsDeleted = 0
join {{ source("assessment_models", "Authority") }} a
on a.Id = ap.AuthorityId and a.IsDeleted = 0
)
, Linked_Authorities as (
select l.TenantId, l.Risk_Id, STRING_AGG(CONVERT(NVARCHAR(max), l.Linked_Authorities), ', ') Linked_Authorities
from distinc_Authorities l
group by
l.TenantId, l.Risk_Id
)

select 
r.*,
o.Risk_OwnerList,
am.Risk_AccessMemberList,
t.Risk_TagsList,
d.Risk_Domain,
d.Risk_DomainValues,
-- cd.Child_Domain, 
-- cd.Child_DomainValues
-- gd.Granchild_Domain, 
-- gd.Granchild_DomainValues
la.Linked_Assets,
li.Linked_Issues,
lm.Linked_Metrics,
lv.Linked_Vendors,
lrr.Linked_RegisterRecords,
las.Linked_Assessments,
lcs.Linked_ControlSets,
lc.Linked_Controls,
lauth.Linked_Authorities,
lp.Linked_Provisions,
concat('Risk name and description: ', r.Risk_Name, ' / ', ' Its status is ', r.Risk_Status, '. This risk is linked to ', la.Linked_Assets,', ', li.Linked_Issues,', ', lm.Linked_Metrics,', ', lv.Linked_Vendors,', ', lrr.Linked_RegisterRecords,', ', las.Linked_Assessments,', ', lcs.Linked_ControlSets,', ', lc.Linked_Controls,', ', lauth.Linked_Authorities,', ', lp.Linked_Provisions) Text

from Risks r
left join Owners o on o.RiskOwner_TenantId = r.TenantId and o.RiskOwner_RiskId = r.Risk_Id
left join AccessMembers am on am.RiskUser_TenantId = r.TenantId and am.RiskUser_RiskId = r.Risk_Id
left join Risk_Tags t on t.TenantId = r.TenantId and t.RiskId = r.Risk_Id
left join domain d on d.TenantId = r.TenantId and d.Risk_Id = r.Risk_Id
-- left join child_domain cd on cd.TenantId = r.TenantId and cd.Risk_Id = r.Risk_Id
-- left join grandchild_domain gd on gd.TenantId = r.TenantId and gd.Risk_Id = r.Risk_Id
left join Linked_Asset la on la.TenantId = r.TenantId and la.Risk_Id = r.Risk_Id
left join Linked_Issue li on li.TenantId = r.TenantId and li.Risk_Id = r.Risk_Id
left join Linked_Metric lm on lm.TenantId = r.TenantId and lm.Risk_Id = r.Risk_Id
left join Linked_Vendor lv on lv.TenantId = r.TenantId and lv.Risk_Id = r.Risk_Id
left join Linked_RegisterRecord lrr on lrr.TenantId = r.TenantId and lrr.Risk_Id = r.Risk_Id
left join Linked_Assessment las on las.TenantId = r.TenantId and las.Risk_Id = r.Risk_Id
left join Linked_ControlSet lcs on lcs.TenantId = r.TenantId and lcs.Risk_Id = r.Risk_Id
left join Linked_Controls lc on lc.TenantId = r.TenantId and lc.Risk_Id = r.Risk_Id
left join Linked_Authorities lauth on lauth.TenantId = r.TenantId and lauth.Risk_Id = r.Risk_Id
left join Linked_Provisions lp on lp.TenantId = r.TenantId and lp.Risk_Id = r.Risk_Id
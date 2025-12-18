with Risks as(
select
r.TenantId,
abp.Name Tenant_Name,
r.Id Risk_Id,
r.TenantEntityUniqueId Risk_IdRef,
r.Name Risk_Name,
r.[Description] Risk_Description,
r.CreationTime Risk_CreationTime,
au2.Name+' '+au2.Surname Risk_CreatedBy,
r.LastModificationTime Risk_LastUpdatedTime,
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

where r.IsDeleted = 0
and abp.IsDeleted = 0 and abp.IsActive = 1
and r.[Status] != 100
)
, Owners as (
select rof.RiskOwner_TenantId, rof.RiskOwner_RiskId, STRING_AGG(rof.OwnerText, ', ') Risk_OwnerList
from {{ ref("vwRiskOwnerFilter") }} rof
group by
rof.RiskOwner_TenantId, rof.RiskOwner_RiskId
)
,  AccessMembers as (
select ruf.RiskUser_TenantId, ruf.RiskUser_RiskId, STRING_AGG(ruf.UserText, ', ') Risk_AccessMemberList
from {{ ref("vwRiskUserFilter") }} ruf
group by
ruf.RiskUser_TenantId, ruf.RiskUser_RiskId
)
, Risk_Tags as (
select rt.TenantId, rt.RiskId, STRING_AGG(t.Name, ', ') Risk_TagsList
from {{ source("risk_models", "RiskTag") }} rt
left join {{ source("assessment_models", "Tags") }} t on t.Id = rt.TagId
and t.TenantId = rt.TenantId and t.IsDeleted = 0 and rt.IsDeleted = 0
group by
rt.TenantId, rt.RiskId
)
, domain as(
select distinct
r.TenantId,
r.Name Risk_Name,
r.Id Risk_Id,
tpc.Label Risk_Domain,
tpa.Label Risk_DomainValues,
tpa.Id ThirdPartyAttributes_Id

from {{ source("risk_models", "Risk") }} r
join {{ source("risk_models", "RiskCustomAttributeData") }} rcad
on rcad.RiskId = r.Id
join {{ source("issue_models", "ThirdPartyAttributes") }} tpa
on tpa.Id = rcad.ThirdPartyAttributesId
join {{ source("issue_models", "ThirdPartyControl") }} tpc
on tpc.Id = tpa.ThirdPartyControlId and tpc.Name = 'RiskDomain'
)
, child_domain as (
select 
d.TenantId, d.Risk_Id,
tpc2.Label Child_Domain,
tpa2.Label Child_DomainValues,
tpa2.Id ThirdPartyAttributes_Id2
from domain d
join {{ source("risk_models", "RiskCustomAttributeData") }} rcad2
on rcad2.RiskId = d.Risk_Id
left join {{ source("issue_models", "ThirdPartyAttributes") }} tpa2
on tpa2.Id = rcad2.ThirdPartyAttributesId
and tpa2.ParentThirdPartyAttributeId = d.ThirdPartyAttributes_Id
left join {{ source("issue_models", "ThirdPartyControl") }} tpc2
on tpc2.Id = tpa2.ThirdPartyControlId
where tpa2.Label is not null
)
, grandchild_domain as (
select 
d.TenantId, d.Risk_Id,
tpc3.Label Granchild_Domain,
tpa3.Label Granchild_DomainValues,
tpa3.Id ThirdPartyAttributes_Id3
from child_domain d
join {{ source("risk_models", "RiskCustomAttributeData") }} rcad3
on rcad3.RiskId = d.Risk_Id
left join {{ source("issue_models", "ThirdPartyAttributes") }} tpa3
on tpa3.Id = rcad3.ThirdPartyAttributesId
and tpa3.ParentThirdPartyAttributeId = d.ThirdPartyAttributes_Id2
left join {{ source("issue_models", "ThirdPartyControl") }} tpc3
on tpc3.Id = tpa3.ThirdPartyControlId
where tpa3.Label is not null
)

select 
r.*, 
o.Risk_OwnerList, 
am.Risk_AccessMemberList, 
t.Risk_TagsList,
d.Risk_Domain,
d.Risk_DomainValues,
cd.Child_Domain, 
cd.Child_DomainValues,
gd.Granchild_Domain, 
gd.Granchild_DomainValues

from Risks r
left join Owners o on o.RiskOwner_TenantId = r.TenantId and o.RiskOwner_RiskId = r.Risk_Id
left join AccessMembers am on am.RiskUser_TenantId = r.TenantId and am.RiskUser_RiskId = r.Risk_Id
left join Risk_Tags t on t.TenantId = r.TenantId and t.RiskId = r.Risk_Id
left join domain d on d.TenantId = r.TenantId and d.Risk_Id = r.Risk_Id
left join child_domain cd on cd.TenantId = r.TenantId and cd.Risk_Id = r.Risk_Id
left join grandchild_domain gd on gd.TenantId = r.TenantId and gd.Risk_Id = r.Risk_Id
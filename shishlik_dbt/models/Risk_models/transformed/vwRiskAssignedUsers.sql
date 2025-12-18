with base as (
select
r.Risk_TenantId,
r.Risk_Id,
r.Risk_Name,
r.Risk_SnapshotDate Risk_LastModificationTime
from {{ ref("vwRisk") }} r
where r.Risk_Status = 1
)
, ris_own as (
select ro.RiskOwner_TenantId, ro.RiskOwner_RiskId, au.AbpUsers_FullName, au.AbpUsers_EmailAddress, coalesce(ro.RiskOwner_LastModificationTime, ro.RiskOwner_CreationTime) as Record_LastModificationTime
from {{ ref("vwRiskOwner") }} ro
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = ro.RiskOwner_UserId
)
, ris_acm as (
select ru.RiskUser_TenantId, ru.RiskUser_RiskId, au.AbpUsers_FullName, au.AbpUsers_EmailAddress, coalesce(ru.RiskUser_LastModificationTime, ru.RiskUser_CreationTime) as Record_LastModificationTime
from {{ ref("vwRiskUser") }} ru
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = ru.RiskUser_UserId
)
, ris_users as (
select
b.Risk_TenantId TenantId,
ro.AbpUsers_FullName UserName,
ro.AbpUsers_EmailAddress UserEmail,
ro.Record_LastModificationTime,
'Owner' Module,
'Risk' AssignedItemType,
b.Risk_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
left join ris_own ro on ro.RiskOwner_RiskId = b.Risk_Id
where ro.AbpUsers_FullName is not null

union all

select
b.Risk_TenantId TenantId,
ru.AbpUsers_FullName UserName,
ru.AbpUsers_EmailAddress UserEmail,
ru.Record_LastModificationTime,
'Access Member' Module,
'Risk' AssignedItemType,
b.Risk_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
left join ris_acm ru on ru.RiskUser_RiskId = b.Risk_Id
where ru.AbpUsers_FullName is not null
)
, ris_trt_plan_asg as (
select 
rtpa.RiskTreatmentPlanAssociation_TenantId TenantId,
rtpa.RiskTreatmentPlanAssociation_RiskId,
rtp.RiskTreatmentPlan_TreatmentName,
au.AbpUsers_FullName,
au.AbpUsers_EmailAddress,
coalesce(rtpo.RiskTreatmentPlanOwner_LastModificationTime, rtpo.RiskTreatmentPlanOwner_CreationTime) as Record_LastModificationTime
from {{ ref("vwRiskTreatmentPlanAssociation") }} rtpa
join {{ ref("vwRiskTreatmentPlan") }} rtp
on rtp.RiskTreatmentPlan_Id = rtpa.RiskTreatmentPlanAssociation_RiskTreatmentPlanId
join {{ ref("vwRiskTreatmentPlanOwner") }} rtpo
on rtpo.RiskTreatmentPlanOwner_RiskTreatmentPlanId  = rtp.RiskTreatmentPlan_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = rtpo.RiskTreatmentPlanOwner_UserId
where rtp.RiskTreatmentPlan_IsDeprecated = 0
)
, ris_trt_plan_user as (
select
b.Risk_TenantId TenantId,
rtp.AbpUsers_FullName UserName,
rtp.AbpUsers_EmailAddress UserEmail,
'Assignee' Module,
'Risk Treatment Plan' AssignedItemType,
rtp.RiskTreatmentPlan_TreatmentName AssignedItemName,
'Risk'+ ' > ' + 'Risk Treatment Plan' AssignedItemParentType,
b.Risk_Name+ ' > '+ rtp.RiskTreatmentPlan_TreatmentName AssignedItemParentName,
Record_LastModificationTime
from base b
join ris_trt_plan_asg rtp
on rtp.RiskTreatmentPlanAssociation_RiskId = b.Risk_Id
where rtp.AbpUsers_FullName is not null
)
, ris_rvw_own as (
select
rr.RiskReview_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
'Owner' Module,
'Risk Review' AssignedItemType,
rr.RiskReview_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName,
coalesce(rro.RiskReviewOwner_LastModificationTime, rro.RiskReviewOwner_CreationTime) as Record_LastModificationTime
from {{ ref("vwRiskReview") }} rr
join {{ ref("vwRiskReviewOwner") }} rro
on rro.RiskReviewOwner_RiskReviewId = rr.RiskReview_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = rro.RiskReviewOwner_UserId
where au.AbpUsers_FullName is not null
)
, final as (
select
TenantId,
UserName,
UserEmail,
Record_LastModificationTime,
Module,
AssignedItemType,
AssignedItemName,
AssignedItemParentType,
AssignedItemParentName
from ris_users

union all

select
TenantId,
UserName,
UserEmail,
Record_LastModificationTime,
Module,
AssignedItemType,
AssignedItemName,
AssignedItemParentType,
AssignedItemParentName
from ris_trt_plan_user

union all

select
TenantId,
UserName,
UserEmail,
Record_LastModificationTime,
Module,
AssignedItemType,
AssignedItemName,
AssignedItemParentType,
AssignedItemParentName
from ris_rvw_own
)

select * from final
with base as (
select
tv.TenantVendor_TenantId,
tv.TenantVendor_Id,
tv.TenantVendor_Name
from {{ ref("vwTenantVendor") }}  tv
where tv.TenantVendor_IsArchived = 0
)
, tv_own as (
select
b.TenantVendor_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(tvo.TenantVendorOwner_LastModificationTime, tvo.TenantVendorOwner_CreationTime) as Record_LastModificationTime,
'Owner' Module,
'Third-Parties' AssignedItemType,
b.TenantVendor_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
join {{ ref("vwTenantVendorOwner") }}  tvo
on tvo.TenantVendorOwner_TenantVendorId = b.TenantVendor_Id
join {{ ref("vwAbpUser") }}  au
on au.AbpUsers_Id = tvo.TenantVendorOwner_UserId
where au.AbpUsers_FullName is not null
)
, tv_acm as (
select
b.TenantVendor_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(tvu.TenantVendorUser_LastModificationTime, tvu.TenantVendorUser_CreationTime) as Record_LastModificationTime,
'Respondent' Module,
'Third-Parties' AssignedItemType,
b.TenantVendor_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
join {{ ref("vwTenantVendorUser") }}  tvu
on tvu.TenantVendorUser_TenantVendorId = b.TenantVendor_Id
join {{ ref("vwAbpUser") }}  au
on au.AbpUsers_Id = tvu.TenantVendorUser_UserId
where au.AbpUsers_FullName is not null
)
, tp_form as (
select
tp.ThirdPartyOnboardingForm_TenantId,
tp.ThirdPartyOnboardingForm_Id,
tp.ThirdPartyOnboardingForm_Name
from {{ ref("vwThirdPartyOnboardingForm") }}  tp
where tp.ThirdPartyOnboardingForm_IsArchived = 0
)
, tp_form_rsp as(
select
tp.ThirdPartyOnboardingForm_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(tr.ThirdPartyOnboardingFormAssessmentRespondent_LastModificationTime, tr.ThirdPartyOnboardingFormAssessmentRespondent_CreationTime) as Record_LastModificationTime,
'Respondent' Module,
'Third-Party Forms' AssignedItemType,
tp.ThirdPartyOnboardingForm_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from tp_form tp
join {{ ref("vwThirdPartyOnboardingFormAssessmentRespondent") }}  tr
on tr.ThirdPartyOnboardingFormAssessmentRespondent_FormId = tp.ThirdPartyOnboardingForm_Id
join {{ ref("vwAbpUser") }}  au
on au.AbpUsers_Id = tr.ThirdPartyOnboardingFormAssessmentRespondent_UserId
)
, tp_form_ass_own as(
select
tp.ThirdPartyOnboardingForm_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(tr.ThirdPartyOnboardingFormOwner_LastModificationTime, tr.ThirdPartyOnboardingFormOwner_CreationTime) as Record_LastModificationTime,
'Assessment Owner' Module,
'Third-Party Forms' AssignedItemType,
tp.ThirdPartyOnboardingForm_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from tp_form tp
join {{ ref("vwThirdPartyOnboardingFormOwner") }}  tr
on tr.ThirdPartyOnboardingFormOwner_FormId = tp.ThirdPartyOnboardingForm_Id
join {{ ref("vwAbpUser") }}  au
on au.AbpUsers_Id = tr.ThirdPartyOnboardingFormOwner_UserId
where ThirdPartyOnboardingFormOwner_OwnerType = 1--Assessment Owners
)
, tp_form_not_form_sub as(
select
tp.ThirdPartyOnboardingForm_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(tr.ThirdPartyOnboardingFormNotification_LastModificationTime, tr.ThirdPartyOnboardingFormNotification_CreationTime) as Record_LastModificationTime,
'Form Submission User' Module,
'Third-Party Forms' AssignedItemType,
tp.ThirdPartyOnboardingForm_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from tp_form tp
join {{ ref("vwThirdPartyOnboardingFormNotification") }}  tr
on tr.ThirdPartyOnboardingFormNotification_FormId = tp.ThirdPartyOnboardingForm_Id
join {{ ref("vwAbpUser") }}  au
on au.AbpUsers_Id = tr.ThirdPartyOnboardingFormNotification_UserId
where tr.ThirdPartyOnboardingFormNotification_NotificationUserType = 1
)
, tp_form_ass_cmp_usr as(
select
tp.ThirdPartyOnboardingForm_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(tr.ThirdPartyOnboardingFormNotification_LastModificationTime, tr.ThirdPartyOnboardingFormNotification_CreationTime) as Record_LastModificationTime,
'Assessment Completion User' Module,
'Third-Party Forms' AssignedItemType,
tp.ThirdPartyOnboardingForm_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from tp_form tp
join {{ ref("vwThirdPartyOnboardingFormNotification") }}  tr
on tr.ThirdPartyOnboardingFormNotification_FormId = tp.ThirdPartyOnboardingForm_Id
join {{ ref("vwAbpUser") }}  au
on au.AbpUsers_Id = tr.ThirdPartyOnboardingFormNotification_UserId
where tr.ThirdPartyOnboardingFormNotification_NotificationUserType = 2
)
, tp_form_not_tp_own as(
select
tp.ThirdPartyOnboardingForm_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(tr.ThirdPartyOnboardingFormNotification_LastModificationTime, tr.ThirdPartyOnboardingFormNotification_CreationTime) as Record_LastModificationTime,
'ThirdParty Owners' Module,
'Third-Party Forms' AssignedItemType,
tp.ThirdPartyOnboardingForm_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from tp_form tp
join {{ ref("vwThirdPartyOnboardingFormNotification") }}  tr
on tr.ThirdPartyOnboardingFormNotification_FormId = tp.ThirdPartyOnboardingForm_Id
join {{ ref("vwAbpUser") }}  au
on au.AbpUsers_Id = tr.ThirdPartyOnboardingFormNotification_UserId
where tr.ThirdPartyOnboardingFormNotification_NotificationUserType = 3
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
from tv_own

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
from tv_acm

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
from tp_form_rsp

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
from tp_form_ass_own

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
from tp_form_not_form_sub

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
from tp_form_ass_cmp_usr

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
from tp_form_not_tp_own
)

select * from final
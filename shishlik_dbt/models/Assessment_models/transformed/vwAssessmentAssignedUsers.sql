with base as (
select
a.Assessment_TenantId,
a.Assessment_ID,
a.Assessment_Name
from {{ ref("vwAssessment") }} a
WHERE a.Assessment_IsTemplate = 1 and a.Assessment_Status != 100 and a.Assessment_IsArchived = 0
)
, ass_temp_own as (
select
b.Assessment_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(ao.AssessmentOwner_LastModificationTime, ao.AssessmentOwner_CreationTime) as Record_LastModificationTime,
'Owner' Module,
'Assessment Templates' AssignedItemType,
b.Assessment_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
join {{ ref("vwAssessmentOwner") }} ao
on ao.AssessmentOwner_AssessmentId = b.Assessment_ID
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = ao.AssessmentOwner_UserId
)
, ass_temp_acm as (
select
b.Assessment_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(aam.AssessmentAccessMember_LastModificationTime, aam.AssessmentAccessMember_CreationTime) as Record_LastModificationTime,
'Access Member' Module,
'Assessment Templates' AssignedItemType,
b.Assessment_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
join {{ ref("vwAssessmentAccessMember") }} aam
on aam.AssessmentAccessMember_AssessmentId = b.Assessment_ID
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = aam.AssessmentAccessMember_UserId
)
, assess as (
select
a.Assessment_TenantId,
a.Assessment_ID,
a.Assessment_Name
from {{ ref("vwAssessment") }} as a
WHERE a.Assessment_IsTemplate = 0
and a.Assessment_IsDeprecatedAssessmentVersion = 0 and a.Assessment_IsArchived = 0
)
, ass_own as (
select
a.Assessment_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(ao.AssessmentOwner_LastModificationTime, ao.AssessmentOwner_CreationTime) as Record_LastModificationTime,
'Owner' Module,
'Assessments' AssignedItemType,
a.Assessment_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from assess a
join {{ ref("vwAssessmentOwner") }} ao
on ao.AssessmentOwner_AssessmentId = a.Assessment_ID
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = ao.AssessmentOwner_UserId
)
, ass_acm as (
select
a.Assessment_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(aam.AssessmentAccessMember_LastModificationTime, aam.AssessmentAccessMember_CreationTime) as Record_LastModificationTime,
'Access Member' Module,
'Assessments' AssignedItemType,
a.Assessment_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from assess a
join {{ ref("vwAssessmentAccessMember") }} aam
on aam.AssessmentAccessMember_AssessmentId = a.Assessment_ID
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = aam.AssessmentAccessMember_UserId
)
, ass_resp as (
select
a.Assessment_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(ar.AssessmentRespondentPermission_LastModificationTime, ar.AssessmentRespondentPermission_CreationTime) as Record_LastModificationTime,
'Assessment Respondent' Module,
'Assessments' AssignedItemType,
a.Assessment_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from assess a
join {{ ref("vwAssessmentRespondentPermission") }} ar
on ar.AssessmentRespondentPermission_AssessmentId = a.Assessment_ID
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = ar.AssessmentRespondentPermission_UserId
)
, ques_rvw as (
select 
a.Assessment_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(qu.QuestionUser_LastModificationTime, qu.QuestionUser_CreationTime) as Record_LastModificationTime,
'Question Reviewer' Module,
'Questions' AssignedItemType,
q.Question_Name AssignedItemName,
'Assessments'+ ' > ' + 'Questions' AssignedItemParentType,
a.Assessment_Name+ ' > '+ q.Question_Name AssignedItemParentName
from assess a
join {{ ref("vwAssessmentResponse") }} ar
on ar.AssessmentResponse_AssessmentId = a.Assessment_ID
join {{ ref("vwAssessment") }} ass on ass.Assessment_Id = ar.AssessmentResponse_AssessmentId
join {{ ref("vwAssessmentDomain") }} ad on ass.Assessment_Id = ad.AssessmentDomain_AssessmentId
join {{ ref("vwQuestion") }} q on ad.AssessmentDomain_Id = q.Question_AssessmentDomainId
join {{ ref("vwQuestionUser") }} qu on qu.QuestionUser_QuestionId = q.Question_Id
join {{ ref("vwAbpUser") }} au on qu.QuestionUser_UserId = au.AbpUsers_Id
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
from ass_temp_own

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
from ass_temp_acm

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
from ass_own

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
from ass_acm

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
from ass_resp

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
from ques_rvw
)

select * from final
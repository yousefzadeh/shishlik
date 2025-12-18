select distinct
au.TenantName
-- ,au.AbpUsers_UserName UserName
,tv.Name TenantVendor_Name
,a.[Id] Assessment_Id
,a.[CreationTime] Assessment_CreationTime
,a.[CreatorUserId] Assessment_CreatorUserId
,a.[LastModificationTime] Assessment_LastModificationTime
,a.[LastModifierUserId] Assessment_LastModifierUserId
,a.[DeleterUserId] Assessment_DeleterUserId
,a.[DeletionTime] Assessment_DeletionTime
,a.[TenantId] Assessment_TenantId
,a.[Name] Assessment_Name
,atag.Assessment_TagName
,aof.OwnerName Assessment_Owner
,a.[Status] Assessment_Status
,case
when a.[Status] = 1
then 'Draft'
when a.[Status] = 2
then 'Approved'
when a.[Status] = 3
then 'Published'
when a.[Status] = 4
then 'Completed'
when a.[Status] = 5
then 'Closed'
when a.[Status] = 6
then 'Reviewed'
when a.[Status] = 7
then 'In Progress'
when a.[Status] = 8
then 'Cancelled'
else 'Undefined'
end as Assessment_StatusCode
,a.[Description] Assessment_Description
,a.[DueDate] Assessment_DueDate
,a.[TenantVendorId] Assessment_TenantVendorId
,a.[PublishedDate] Assessment_PublishedDate
,a.[EndPage] Assessment_EndPage
,a.[Introduction] Assessment_Introduction
,a.[TypeId] Assessment_TypeId
,a.[AuthorityId] Assessment_AuthorityId
,a.[PolicyId] Assessment_PolicyId
,a.[QuestionType] Assessment_QuestionType
,case
when a.[QuestionType] = 0
then 'Preferred Answer'
when a.[QuestionType] = 1
then 'Weighted Score'
when a.[QuestionType] = 2
then 'Risk Rated'
else 'Undefined'
end Assessment_QuestionTypeCode
,a.[CreatedFromTemplateId] Assessment_CreatedFromTemplateId
,a.[BulkSendAssessmentLogId] Assessment_BulkSendAssessmentLogId
,a.[HasPeriod] Assessment_HasPeriod
,a.[Period] Assessment_Period
,a.[PeriodicAssessmentId] Assessment_PeriodicAssessmentId
,a.[PeriodStartDate] Assessment_PeriodStartDate
,a.[ReAssessmentParentId] Assessment_ReAssessmentParentId
,a.[ReAssessmentRootId] Assessment_ReAssessmentRootId
,a.[ReAssessmentVersion] Assessment_ReAssessmentVersion
,a.[PublishedById] Assessment_PublishedById
,a.[ClosedDate] Assessment_ClosedDate
,a.[ClosedReason] Assessment_ClosedReason
,a.[ResponseCompletedDate] Assessment_ResponseCompletedDate
,a.[ResponseStartedDate] Assessment_ResponseStartedDate
,a.[ReviewedDate] Assessment_ReviewedDate
,a.[AssessmentVersion] Assessment_AssessmentVersion
,a.[CreatedFromAssessmentId] Assessment_CreatedFromAssessmentId
,a.[WorkFlowId] Assessment_WorkFlowId
,case
when a.[WorkFlowId] = 1 then 'Requirement' when [WorkFlowId] = 0 then 'Question' else 'Undefined'
end Assessment_Workflow
,a.[IsLocked] Assessment_IsLocked
,a.[CreatedFromMethod] Assessment_CreatedFromMethod
,a.[CreatedFromTemplateStatus] Assessment_CreatedFromTemplateStatus
,a.[IsLockedForClients] Assessment_IsLockedForClients
,a.[NameVarChar] Assessment_NameVarChar
,a.[CreatedFromOnboardingFormId] Assessment_CreatedFromOnboardingFormId
,a.[IsAutomatedAssessment] Assessment_IsAutomatedAssessment
,ufa.IsFavourite Assessment_IsFavourite
,au2.AbpUsers_UserName UserSelectedFavourite
from {{ source("assessment_models", "Assessment") }} a
join {{ ref("vwActiveUsers") }} au
on au.TenantId = a.TenantId
join {{ source("tenant_models", "TenantVendor") }} tv
on tv.Id = a.TenantVendorId and tv.TenantId = a.TenantId and tv.IsDeleted = 0
left join {{ source("assessment_models", "UserFavouriteAssessment") }} ufa on ufa.AssessmentId = a.Id and ufa.IsDeleted = 0
left join {{ ref("vwActiveUsers") }} au2 on au2.UserId = ufa.UserId and ufa.TenantId = au2.TenantId
left join {{ ref("vwAssessmentOwnerFilter") }} aof on aof.AssessmentOwner_AssessmentId = a.Id
left join {{ ref("vwAssessmentTag") }} atag on atag.AssessmentTag_AssessmentId = a.Id and atag.AssessmentTag_TenantId = a.TenantId
where a.IsDeleted = 0 and a.IsTemplate = 0
and a.IsDeprecatedAssessmentVersion = 0
and a.Status != 8 and a.IsArchived = 0
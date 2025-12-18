select
a.TenantId,
abp.Name Tenant_Name,
tv.Name TenantVendor_Name,
a.Id Assessment_Id,
cast(a.IsDeleted as int) IsDeleted,
a.Name Assessment_Name,
a.Description Assessment_Description,
a.DueDate Assessment_DueDate,
case
when a.[Status] in (4, 5, 6) then 'No'
when a.[Status] not in (4, 5, 6) and a.DueDate > getdate() then 'No'
when a.[Status] not in (4, 5, 6) and a.DueDate is null then 'Yes'
when a.[Status] not in (4, 5, 6) and a.DueDate < getdate() then 'Yes'
else 'No'
end Assessment_IsOverDue,
aof.OwnerList Assessment_Owner,
a.CreationTime Assessment_CreationTime,
COALESCE(a.LastModificationTime, a.CreationTime) Assessment_LastModificationTime,
a.PublishedDate Assessment_PublishedDate,
case
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
end as Assessment_Status,
atag.Assessment_TagList Assessment_Tags,
e.Name Assessment_ProductType,
case
when a.QuestionType = 0
then 'Preferred Answer'
when a.QuestionType = 1
then 'Weighted Score'
when a.QuestionType = 2
then 'Risk Rated'
else 'Undefined'
end Assessment_Style,
au.Name+' '+au.Surname Assessment_PublishedBy,
a.ClosedDate Assessment_ClosedDate,
a.ResponseCompletedDate Assessment_ResponseCompletedDate,
a.ResponseStartedDate Assessment_ResponseStartedDate,
a.ReviewedDate Assessment_ReviewedDate,
case
when a.WorkFlowId = 1 then 'RBA' when a.WorkFlowId = 0 then 'QBA' else 'Undefined'
end Assessment_Workflow,
auth.Name Assessment_LinkedAuthority,
p.Name Assessment_LinkedControlSet,
concat('Assessment name and description: ', a.[Name], ' / ', ' Its status is ', case
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
end, '. This assessment is linked to ', auth.[Name] , ' and ', p.[Name]) Text

from {{ source("assessment_models", "Assessment") }} a
join {{ source("assessment_models", "AbpTenants") }} abp on abp.Id = a.TenantId
join {{ source("tenant_models", "TenantVendor") }} tv on tv.Id = a.TenantVendorId and tv.TenantId = a.TenantId and tv.IsDeleted = 0
left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = a.PublishedById and au.TenantId = a.TenantId and au.IsDeleted = 0 and au.IsActive = 1
left join {{ ref("vwAssessmentTagList") }} atag on atag.Assessment_Id = a.Id-- and atag.AssessmentTag_TenantId = a.TenantId
left join {{ ref("vwAssessmentOwnerList") }} aof on aof.AssessmentOwner_AssessmentId = a.Id
left join {{ source("engagement_models", "Engagement") }} e on e.Id = a.EngagementId and e.TenantId = a.TenantId
left join {{ source("assessment_models", "Authority") }} auth on auth.Id = a.AuthorityId and auth.IsDeleted = 0
left join {{ source("assessment_models", "Policy") }} p on p.Id = a.PolicyId and p.IsDeleted = 0

where a.IsTemplate = 0
and a.IsDeprecatedAssessmentVersion = 0
and a.Status != 8 and a.IsArchived = 0
and abp.IsDeleted = 0 and abp.IsActive = 1
select distinct 
a.TenantId,
abp.Name Tenant_Name,
tv.Name TenantVendor_Name,
case when temp.Name is NULL then 'No Template' else temp.Name end Template_Name,
a.Id Assessment_Id,
a.IsDeleted,
a.Name Assessment_Name,
a.[Description] Assessment_Description,
a.DueDate Assessment_DueDate,
aof.OwnerName Assessment_Owner,
a.CreationTime Assessment_CreationTime,
a.LastModificationTime Assessment_LastModificationTime,
a.publishedDate Assessment_PublishedDate,
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
atag.Assessment_TagName Assessment_Tags,
a.EndPage Assessment_EndPage,
a.Introduction Assessment_Introduction,
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
a.ClosedReason Assessment_ClosedReason,
a.ResponseCompletedDate Assessment_ResponseCompletedDate,
a.ResponseStartedDate Assessment_ResponseStartedDate,
a.ReviewedDate Assessment_ReviewedDate,
case
when a.WorkFlowId = 1 then 'Requirement Based' when a.WorkFlowId = 0 then 'Questionnaire Based' else 'Undefined'
end Assessment_Workflow,
case when a.IsAutomatedAssessment = 1 then 'Yes' else 'No' end Assessment_IsAutomatedAssessment,
case when ufa.IsFavourite = 1 then 'Yes' else 'No' end Assessment_IsFavourite,
au2.Name+' '+au2.Surname User_SelectedFavourite,
ad.Id AssessmentDomain_Id,
ad.Name AssessmentDomain_Name,
ad.[Description] AssessmentDomain_Description,
ad.[Order] AssessmentDomain_Order,
q.Id Question_Id,
q.IdRef Question_IdRef,
q.Name Question_Name,
q.[Description] Question_Description,
q.[Order] Question_Order,
case
when q.[Type] = 1
then 'Yes No'
when q.[Type] in (2, 5, 6, 10)
then 'Choose One'
when q.[Type] in (3, 7, 8)
then 'Choose Many'
when q.[Type] in (4, 9)
then 'Free Text Response'
else 'Undefined'
end as Question_Type,
case when q.IsMandatory = 1 then 'Yes' else 'No' end Question_IsMandatory,
qtj.TagName_List Question_Tags,
qtr.Question_Name Question_TargetName,
qtr.Question_TargetResponse,
qtr.Question_TargetScore,
qtr.Question_TargetRiskStatusCode Question_TargetRiskStatus,
ans.Answer_Id,
ans.Answer_Combined Answer_Response,
ans.Answer_TextArea,
ans.Answer_StatusCode Answer_Status,
ans.Answer_RiskStatusCode Answer_RiskStatus,
ans.Answer_Score,
ans.Answer_MaxPossibleScore,
ans.Answer_ReviewerComment,
case
when ans.Answer_Compliance = 1 then 'Compliant'
when ans.Answer_Compliance = 2 then 'Not Compliant'
else 'None'
end
Answer_Compliance,
ar.Name AssessmentResponse_Name,
case
ar.Status
when 1
then 'Published'
when 2
then 'In Progress'
when 3
then 'Completed'
when 4
then 'Closed'
when 100
then 'UnSent'
end AssessmentResponse_Status,
ar.SubmittedDate AssessmentResponse_SubmittedDate,
au3.Name+' '+au3.Surname Assessment_ResponderName,
ar.InternalSummary AssessmentResponse_InternalSummary,
ar.ExternalSummary AssessmentResponse_ExternalSummary,
ar.Score Assessment_OverallScore,
quf.ReviewerList Question_AssignedReviewer,
pal.Assessment_LinkedAuthority,
pal.Question_LinkedProvisionRefIdList,
pal.Question_LinkedProvisionList,
pal.Assessment_LinkedControlSet,
pal.Question_LinkedControlRefIdList,
pal.Question_LinkedControlList,
pal.Question_LinkedRiskList,
pal.Question_LinkedIssueList


from {{ source("assessment_models", "Assessment") }} a
join {{ source("assessment_models", "AbpTenants") }} abp on abp.Id = a.TenantId
join {{ source("tenant_models", "TenantVendor") }} tv on tv.Id = a.TenantVendorId and tv.TenantId = a.TenantId and tv.IsDeleted = 0
left join {{ source("assessment_models", "Assessment") }} temp on temp.Id = a.CreatedFromTemplateId
left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = a.PublishedById and au.TenantId = a.TenantId and au.IsDeleted = 0 and au.IsActive = 1
left join {{ ref("vwAssessmentTag") }} atag on atag.AssessmentTag_AssessmentId = a.Id and atag.AssessmentTag_TenantId = a.TenantId
left join {{ ref("vwAssessmentOwnerFilter") }} aof on aof.AssessmentOwner_AssessmentId = a.Id
left join {{ source("assessment_models", "UserFavouriteAssessment") }} ufa on ufa.AssessmentId = a.Id and ufa.IsDeleted = 0
left join {{ source("assessment_models", "AbpUsers") }} au2 on au2.Id = ufa.UserId and ufa.TenantId = au2.TenantId and au2.IsDeleted = 0 and au2.IsActive = 1
left join {{ source("engagement_models", "Engagement") }} e on e.Id = a.EngagementId and e.TenantId = a.TenantId
join {{ source("assessment_models", "AssessmentDomain") }} ad on ad.AssessmentId = a.Id and ad.TenantId = a.TenantId and ad.IsDeleted = 0
join {{ source("assessment_models", "Question") }} q on q.AssessmentDomainId = ad.Id and q.TenantId = ad.TenantId and q.IsDeleted = 0
left join {{ ref("vwQuestionTagsJoined") }} qtj on qtj.QuestionTags_QuestionId = q.Id and qtj.Tags_TenantId = q.TenantId
left join {{ ref("vwQuestionTargetResponse") }} qtr on qtr.Question_ID = q.Id and qtr.Question_TenantId = q.TenantId
left join {{ ref("vwAnswer") }} ans on ans.Answer_QuestionId = q.Id and ans.Answer_TenantId = q.TenantId
left join {{ source("assessment_models", "AssessmentResponse") }} ar on ar.AssessmentId = a.Id and ar.TenantId = a.TenantId and ar.IsDeleted = 0
left join {{ source("assessment_models", "AbpUsers") }} au3 on au3.Id = ar.UserId and au3.TenantId = ar.TenantId
left join {{ ref("vwQuestionUserFilter") }} quf on quf.QuestionUser_QuestionId = q.Id and quf.QuestionUser_TenantId = q.TenantId
left join {{ ref("prim_Assessment_LinkedData") }} pal on pal.TenantId = a.TenantId and pal.Assessment_Id = a.Id and pal.Question_Id = q.Id
where a.IsDeleted = 0 and a.IsTemplate = 0
and a.IsDeprecatedAssessmentVersion = 0
and a.Status != 8 and a.IsArchived = 0
and abp.IsDeleted = 0 and abp.IsActive = 1
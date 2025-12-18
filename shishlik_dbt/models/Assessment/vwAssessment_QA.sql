select distinct
ua.Assessment_Id,
ua.Assessment_TenantId,
ua.TenantName,
-- ua.UserName,
ua.Assessment_Name,
ad.Id AssessmentDomain_Id,
ad.Name AssessmentDomain_Name,
ad.Description AssessmentDomain_Description,
q.Id Question_Id,
q.IdRef Question_IdRef,
q.Name Question_Name,
q.[Description] Question_Description,
q.[Order] Question_Order,
q.[Type] Question_Type,
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
end as [Question_TypeCode],
q.ComponentStr Question_ComponentStr,
q.VendorDocumentRequired Question_VendorDocumentRequired,
q.Weighting Question_Weighting,
q.RiskStatus Question_RiskStatus,
case
when q.HiddenInSurveyForConditional = 1 and a.Answer_ResponderId is NULL
then 'Skip Logic Applied'
when a.Answer_Submit is NULL
then 'Not Answered'
else 'Responded'
end
Question_Status,
q.HasConditionalLogic Question_HasConditionalLogic,
q.IsVisibleIfConditional Question_IsVisibleIfConditional,
q.HiddenInSurveyForConditional Question_HiddenInSurveyForConditinal,
q.DisplayDocumentUpload Question_DisplayDocumentUpload,
q.QuestionGroupId Question_QuestionGroupId,
q.Suborder Question_Suborder,
q.IsMandatory Question_IsMandatory,
q.IsMultiSelectType Question_IsMultiSelectType,
q.QuestionGroupResponseId Question_QuestionGroupResponseId,
q.IsActive Question_IsActive,
a.Answer_Id,
a.Answer_ComponentStr,
a.Answer_Combined Answer_Response,
a.Answer_Explanation,
a.Answer_TextArea,
a.Answer_Status,
a.Answer_StatusCode,
a.Answer_Score,
a.Answer_MaxPossibleScore,
a.Answer_RiskStatusCalc Answer_RiskStatus,
a.Answer_RiskStatusCode,
a.Answer_ResponderId,
a.Answer_ReviewerComment,
ar.AssessmentResponse_StatusCode,
ar.AssessmentResponse_SubmittedDate,
ar.AssessmentResponse_ExternalSummary,
ar.AssessmentResponse_InternalSummary,
ar.AssessmentResponse_Name,
ar.AssessmentResponse_RiskRatingWeightedScore,
au.AbpUsers_UserName Assessment_ResponderName,
adoc.DocumentFileName,
adoc.DocumentUrl,
adoc.IsHaileySuggestedDocument

from {{ ref("vwUserAssessments") }} ua
join {{ source("assessment_models", "AssessmentDomain") }} ad on ad.AssessmentId = ua.Assessment_Id and ad.IsDeleted = 0
left join {{ source("assessment_models", "Question") }} q on q.AssessmentDomainId = ad.id and q.IsDeleted = 0
left join {{ ref("vwAnswer") }} a on a.Answer_QuestionId = q.Id
left join {{ ref("vwAssessmentResponse") }} ar on ar.AssessmentResponse_AssessmentId = ua.Assessment_Id
left join {{ ref("vwActiveUsers") }} au on au.UserId = ar.AssessmentResponse_UserId
left join {{ source("assessment_models", "AnswerDocument") }} adoc on adoc.AnswerId = a.Answer_Id and adoc.IsDeleted = 0
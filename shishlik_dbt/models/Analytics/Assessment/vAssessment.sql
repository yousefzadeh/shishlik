SELECT
a.uuid,
a.TenantId,
abt.Name TenantName,
t.Id as Template_Id,
case when t.Name is null then 'No Template' else t.Name end Template_Name,
a.Id as Assessment_Id,
a.Name as Assessment_Name,
a.Description as Assessment_Description,
a.Status as Assessment_StatusCode,
case when a.Status = 1 then 'Draft'
	  when a.Status = 2 then 'Approved'
	  when a.Status = 3 then 'Published'
	  when a.Status = 4 then 'Completed'
	  when a.Status = 5 then 'Closed'
	  when a.Status = 6 then 'Reviewed'
	  when a.Status = 7 then 'In Progress'
	  when a.Status = 8 then 'Cancelled'
else 'Undefined' end as Assessment_Status,
a.DueDate as Assessment_DueDate,
a.ResponseCompletedDate as Assessment_ResponseCompletedDate,
a.PublishedDate as Assessment_PublishedDate,
a.QuestionType Assessment_QuestionTypeCode,
case
	  when a.QuestionType = 0 then 'Preferred Answer'
	  when a.QuestionType = 1 then 'Weighted Score'
	  when a.QuestionType = 2 then 'Risk Rated'
else 'Undefined' end as Assessment_QuestionType,
a.PolicyId as Assessment_PolicyId,
a.AuthorityId as Assessment_AuthorityId,
a.WorkFlowId  Assessment_WorkflowCode,
case
	  when a.WorkFlowId = 1 then 'Requirement' 
	  when a.WorkFlowId = 0 then 'Question' else 'Undefined'
end as Assessment_Workflow,
a.TenantVendorId as Assessment_RespondingTeamId,
tv.Name as Assessment_RespondingTeamName,
a.IsTemplate as Assessment_IsTemplate,
a.CreationTime Assessment_CreationTime,
coalesce(a.LastModificationTime, a.CreationTime) as Assessment_LastModificationTime,
a.CreationTime as Assessment_CreationDate,
a.IsAutomatedAssessment as Assessment_IsAutomatedAssessment,
a.CreatedFromTemplateId as Assessment_CreatedFromTemplateId,
a.HasPeriod Assessment_RecurrenceCode,
case when a.HasPeriod = 1 then 'yes' else 'No' end Assessment_Recurrence,
a.Period Assessment_TimePeriodCode,
case 
when a.Period= 4 then '1 week'
when a.Period= 3 then '1 month'
when a.Period= 6 then '3 months'
when a.Period= 2 then '6 months'
when a.Period= 1 then '1 year'
when a.Period= 8 then '2 years'
when a.Period= 9 then '3 years'
end Assessment_TimePeriod,
a.PeriodStartDate Assessment_PeriodStartDate
FROM {{ source("assessment_ref_models", "Assessment") }} as a
left join {{ source("assessment_ref_models", "Assessment") }} t on t.Id = a.CreatedFromTemplateId and t.IsDeleted = 0
JOIN {{ source("third-party_ref_models", "TenantVendor") }} tv ON a.TenantVendorId = tv.Id and tv.IsDeleted = 0
JOIN {{ source("abp_ref_models", "AbpTenants") }} abt ON abt.Id = a.TenantId
WHERE abt.IsDeleted = 0 and abt.IsActive = 1
and a.IsDeleted = 0 AND a.IsTemplate = 0
and a.IsDeprecatedAssessmentVersion = 0 and a.IsArchived = 0
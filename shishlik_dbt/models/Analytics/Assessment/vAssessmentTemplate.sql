SELECT
a.uuid,
a.TenantId,
abt.Name as TenantName,
a.Id as Template_Id,
a.CreationTime as Template_CreationTime,
a.Name as Template_Name,
a.Description as Template_Description,
a.Status Template_StatusCode,
case
	  when a.Status = 1
	  then 'Draft'
	  when a.Status = 2
	  then 'Approved'
	  when a.Status = 3
	  then 'Published'
	  when a.Status = 4
	  then 'Completed'
	  when a.Status = 5
	  then 'Closed'
	  when a.Status = 6
	  then 'Reviewed'
	  when a.Status = 7
	  then 'In Progress'
	  when a.Status = 8
	  then 'Cancelled'
	  else 'Undefined'
end as Template_Status,
a.PublishedDate as Template_PublishedDate,
a.PublishedById as Template_PublishedById,
a.QuestionType Template_QuestionTypeCode,
case
	  when a.QuestionType = 0
	  then 'Preferred Answer'
	  when a.QuestionType = 1
	  then 'Weighted Score'
	  when a.QuestionType = 2
	  then 'Risk Rated'
	  else 'Undefined'
end as Template_QuestionType,
a.PolicyId as Template_PolicyId,
a.AuthorityId as Template_AuthorityId,
a.WorkFlowId Template_WorkflowFlag,
case
	  when a.WorkFlowId = 1 then 'Requirement' 
	  when a.WorkFlowId = 0 then 'Question' else 'Undefined'
end as Template_Workflow,
a.ParentMarketplaceTemplateId as Template_ParentMarketplaceTemplateId,
a.CreatedFromTemplateStatus as Assessment_CreatedFromTemplateStatus,	
coalesce(a.LastModificationTime, a.CreationTime) as Template_LastModificationTime

FROM {{ source("assessment_ref_models", "Assessment") }} as a
JOIN {{ source("abp_ref_models", "AbpTenants") }} abt ON abt.Id = a.TenantId
WHERE abt.IsDeleted = 0 and abt.IsActive = 1
and a.IsDeleted = 0 AND a.IsTemplate = 1 and a.Status != 100 and a.IsArchived = 0
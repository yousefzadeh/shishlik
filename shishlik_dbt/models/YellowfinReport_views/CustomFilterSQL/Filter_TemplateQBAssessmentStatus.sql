{# 
DOC START
  - name: Filter_TemplateQBAssessmentStatus
    description: |
        This view is used in a custom SQL filter for the following View:
        - Question Based Assessment Answer Details
               
        It lists One row per 
        - Template (QBA only)
        - Assessment (QBA only)
        - Unique Assessment Status of all versions of Assessments

    columns:
      - name: Tenant_Id
        description: ID of Login Tenant for Access Filtering
      - name: Template_Name
        description: Name of the Assessment Template 
      - name: Assessment_Name
        description: Name of the Assessment 
      - name: Status
        description: Status of the Assessment versions
      - name: QuestionType
        description: |
          QuestionType specifies if the Assessment is Risk or Weighted Score
          1 for Risk Assessment
          2 for Weighted Score Assessment

DOC END    
#}
with final as (
	SELECT DISTINCT
		asst.Assessment_TenantId Tenant_Id,
		asst.Assessment_Name Template_Name,
		ass.Assessment_Name,
		ass.Assessment_StatusCode Status,
		ass.Assessment_QuestionType QuestionType,
		MAX(GREATEST(cast(asst.Assessment_UpdateTime as datetime2), cast(ass.Assessment_UpdateTime as datetime2))) OVER (PARTITION BY asst.Assessment_TenantId,asst.Assessment_Name,ass.Assessment_Name,ass.Assessment_StatusCode,ass.Assessment_QuestionType) AS Filter_UpdateTime

	FROM
		{{ ref("vwAssessment") }} asst
	INNER JOIN
		{{ ref("vwAssessment") }} ass
		ON
			ass.Assessment_TenantId = asst.Assessment_TenantId
			AND	ass.Assessment_CreatedFromTemplateId = asst.Assessment_ID
			AND	ass.Assessment_IsTemplate = 0
			AND	ass.Assessment_WorkFlowId = 0
	WHERE
	asst.Assessment_IsTemplate = 1
	and ass.Assessment_IsArchived =0
	AND ass.Assessment_Status in (4, 6)
)
select 
  Tenant_Id,
  Template_Name,
  Assessment_Name,
  Status,
  QuestionType,
  Filter_UpdateTime
from final 
{# where Tenant_Id = 1384 #}
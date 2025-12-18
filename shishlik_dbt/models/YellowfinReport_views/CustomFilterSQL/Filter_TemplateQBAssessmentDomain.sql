{# 
DOC START
  - name: Filter_TemplateQBAssessmentDomain
    description: |
        This view is used in a custom SQL filter for the following View:
        - Question Based Assessment Answer Details
               
        It lists One row per 
        - Template (QBA only)
        - Assessment (QBA only)
        - Domains for the Questions

    columns:
      - name: Tenant_Id
        description: ID of Login Tenant for Access Filtering
      - name: Template_Name
        description: Name of the Assessment Template 
      - name: Assessment_Name
        description: Name of the Assessment 
      - name: Domain_Name
        description: Domains of the Questions
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
        ad.AssessmentDomain_Name Domain_Name,
        ass.Assessment_QuestionType QuestionType,
        max(GREATEST(cast(asst.Assessment_UpdateTime as datetime2), cast(ass.Assessment_UpdateTime as datetime2), ISNUll(cast(ad.AssessmentDomain_UpdateTime as datetime2), '2000-01-01 00:00:00'))) over (partition by ass.Assessment_TenantId,asst.Assessment_Name,ass.Assessment_Name, ad.AssessmentDomain_Name, ass.Assessment_QuestionType)  AS Filter_UpdateTime
    FROM
        {{ ref("vwAssessment") }} asst
    INNER JOIN
        {{ ref("vwAssessment") }} ass
        ON
            ass.Assessment_TenantId = asst.Assessment_TenantId
            AND	ass.Assessment_CreatedFromTemplateId = asst.Assessment_ID
            AND	ass.Assessment_IsTemplate = 0
            AND	ass.Assessment_WorkFlowId = 0
    INNER JOIN
        {{ ref("vwAssessmentDomain") }} ad
        ON
            ad.AssessmentDomain_TenantId = ass.Assessment_TenantId
            AND	ad.AssessmentDomain_AssessmentId = ass.Assessment_ID
    WHERE
    asst.Assessment_IsTemplate = 1
    and ass.Assessment_IsArchived =0
    AND ass.Assessment_Status in (4, 6)
)
select 
Tenant_Id,
Template_Name,
Assessment_Name,
Domain_Name,
QuestionType,
Filter_UpdateTime
from final 
{# where Tenant_Id = 1384 #}
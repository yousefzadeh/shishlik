{# 
DOC START
  - name: Filter_TemplateQBAssessmentQuestionTag
    description: |
        This view is used in a custom SQL filter for the following View:
        - Question Based Assessment Answer Details
               
        It lists One row per 
        - Template (QBA only)
        - Assessment (QBA only)
        - Unique Tags attached to the Questions of all versions of Assessments

    columns:
      - name: Tenant_Id
        description: ID of Login Tenant for Access Filtering
      - name: Template_Name
        description: Name of the Assessment Template 
      - name: Assessment_Name
        description: Name of the Assessment 
      - name: QuestionTag_Name
        description: Unique Tags attached to the Questions of all versions of Assessments
      - name: QuestionType
        description: |
          QuestionType specifies if the Assessment is Risk or Weighted Score
          1 for Risk Assessment
          2 for Weighted Score Assessment

DOC END    
#}
with AssessmentDomain as (
  select distinct AssessmentDomain_TenantId, AssessmentDomain_AssessmentId, AssessmentDomain_ID
  from {{ ref("vwAssessmentDomain") }}
)
,   QuestionAnswer as (
  select distinct Question_TenantId, Question_AssessmentDomainId , Question_ID
  from {{ ref("vwQuestionAnswer") }}
)
, QuestionTagsJoined as (
  select distinct QuestionTags_QuestionId, Tags_Name, QuestionTagsJoined_UpdateTime
  from {{ ref("vwQuestionTagsJoined") }}

) 

, final as (
    SELECT DISTINCT
        asst.Assessment_TenantId Tenant_Id,
        asst.Assessment_Name Template_Name,
        ass.Assessment_Name,
        qtj.Tags_Name QuestionTag_Name,
        ass.Assessment_QuestionType QuestionType,
        MAX(GREATEST(cast(asst.Assessment_UpdateTime as datetime2), cast(ass.Assessment_UpdateTime as datetime2), ISNUll(cast(qtj.QuestionTagsJoined_UpdateTime as datetime2), '2000-01-01 00:00:00'))) OVER (PARTITION BY asst.Assessment_TenantId,asst.Assessment_Name,ass.Assessment_Name,isnull(qtj.Tags_Name,''),ass.Assessment_QuestionType) AS Filter_UpdateTime
    FROM
        {{ ref("vwAssessment") }} asst
    INNER JOIN
        {{ ref("vwAssessment") }} ass
        ON ass.Assessment_TenantId = asst.Assessment_TenantId
        AND	ass.Assessment_CreatedFromTemplateId = asst.Assessment_ID
        AND	ass.Assessment_IsTemplate = 0
        AND	ass.Assessment_WorkFlowId = 0
    INNER JOIN AssessmentDomain ad
        ON ad.AssessmentDomain_TenantId = ass.Assessment_TenantId
        AND	ad.AssessmentDomain_AssessmentId = ass.Assessment_ID
    INNER JOIN QuestionAnswer qa
        ON qa.Question_TenantId = ad.AssessmentDomain_TenantId
        AND	qa.Question_AssessmentDomainId = ad.AssessmentDomain_ID
    LEFT JOIN QuestionTagsJoined qtj
        ON qtj.QuestionTags_QuestionId = qa.Question_ID
    
    WHERE asst.Assessment_IsTemplate = 1
      AND ass.Assessment_IsArchived =0
      AND ass.Assessment_Status in (4, 6)
)
select 
  Tenant_Id,
  Template_Name,
  Assessment_Name,
  QuestionTag_Name,
  QuestionType,
  Filter_UpdateTime
from final 
{# where Tenant_Id = 1384 #}
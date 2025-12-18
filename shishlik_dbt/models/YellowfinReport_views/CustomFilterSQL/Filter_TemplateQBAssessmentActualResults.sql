{# 
DOC START
  - name: Filter_TemplateQBAssessmentActualResults
    description: |
      This view is used in a custom SQL filter for the following View:
      - Question Based Assessment Answer Details
              
      It lists One row per 
      - Template
      - Question Based Assessment (Status 4,6) 
      - Actual Results
      For Reqular Question Answer as well as Question Group Answer


    columns:
      - name: Tenant_Id
        description: ID of Login Tenant for Access Filtering
      - name: Template_Name
        description: Name of the Assessment Template 
      - name: Assessment_Name
        description: Name of the Assessment
      - name: ActualResults
        description: Actual Results as entered by the responder
      - name: QuestionType
        description: |
          QuestionType specifies if the Assessment is Risk or Weighted Score
          1 for Risk Assessment
          2 for Weighted Score Assessment
      - name: IsGroupQuestion
        description: |
          Questions are of 2 types:
          0 for Regular Questions in Question table
          1 for Group Questions in GroupQuestion > GroupQuestionResponse tables
          
DOC END    
#}
WITH QuestionAnswer as (
  SELECT DISTINCT 
    Question_AssessmentDomainId,
    Question_TenantId, 
    Answer_ID, 
    Question_QuestionGroupResponseId
  FROM {{ ref("vwQuestionAnswer") }} 
)
, AnswerResponseFilter AS (
  SELECT  DISTINCT
    Answer_Id, 
    Answer_TenantId, 
    AnswerResponse_Value,
    MAX(ARF_Updatetime)  OVER (PARTITION BY  Answer_TenantId, AnswerResponse_Value) MAX_ARF_Updatetime
  FROM {{ ref("vwAnswerResponseFilter") }}
)
, regular_question as (
  SELECT 
    ass.Assessment_TenantId Tenant_Id,
    asst.Assessment_Name Template_Name,
    ass.Assessment_Name,
    arf.AnswerResponse_Value ActualResults,
    ass.Assessment_QuestionType QuestionType,
    0 as IsGroupQuestion,
    GREATEST(cast(asst.Assessment_UpdateTime as datetime2), cast(ass.Assessment_UpdateTime as datetime2), ISNUll(cast(arf.MAX_ARF_Updatetime as datetime2), '2000-01-01 00:00:00')) AS Filter_UpdateTime  
  FROM {{ ref("vwAssessment") }} AS asst
  INNER JOIN {{ ref("vwAssessment") }} AS ass
    ON asst.Assessment_ID = ass.Assessment_CreatedFromTemplateId
    AND asst.Assessment_TenantId = ass.Assessment_TenantId
    AND ass.Assessment_IsTemplate = 0
    AND asst.Assessment_IsTemplate = 1
    AND ass.Assessment_WorkFlowId = 0
    AND ass.Assessment_Status in (4,6)
  INNER JOIN {{ ref("vwAssessmentDomain") }} AS ad
    ON ass.Assessment_ID = ad.AssessmentDomain_AssessmentId
    and ass.Assessment_TenantID = ad.AssessmentDomain_TenantId
  INNER JOIN QuestionAnswer AS qa
    ON ad.AssessmentDomain_ID = qa.Question_AssessmentDomainId
    and ad.AssessmentDomain_TenantID = qa.Question_TenantId
  LEFT JOIN AnswerResponseFilter AS arf
    ON qa.Answer_ID = arf.Answer_Id
    and qa.Question_TenantId = arf.Answer_TenantId
  WHERE asst.Assessment_IsTemplate = 1
    and ass.Assessment_IsArchived = 0
)
, QuestionGroupAnswers AS (
  SELECT  DISTINCT
    QuestionGroupResponse_Id, 
    QuestionGroupResponse_TenantId, 
    QuestionGroupResponse_Response,
    MAX(QGA_Updatetime)  OVER (PARTITION BY  QuestionGroupResponse_TenantId, QuestionGroupResponse_Response) MAX_QGA_Updatetime
  FROM {{ ref("vwQuestionGroupAnswers") }}
)
 , group_question as (
  SELECT 
    ass.Assessment_TenantId Tenant_Id,
    asst.Assessment_Name Template_Name,
    ass.Assessment_Name,
    qga.QuestionGroupResponse_Response ActualResults,
    ass.Assessment_QuestionType QuestionType,
    1 as IsGroupQuestion,
    GREATEST(cast(asst.Assessment_UpdateTime as datetime2), cast(ass.Assessment_UpdateTime as datetime2), ISNUll(cast(qga.MAX_QGA_Updatetime as datetime2), '2000-01-01 00:00:00')) AS Filter_UpdateTime
  FROM {{ ref("vwAssessment") }} AS asst
  INNER JOIN {{ ref("vwAssessment") }} AS ass
    ON asst.Assessment_ID = ass.Assessment_CreatedFromTemplateId
    AND asst.Assessment_TenantId = ass.Assessment_TenantId
    AND ass.Assessment_IsTemplate = 0
    AND asst.Assessment_IsTemplate = 1
    AND ass.Assessment_WorkFlowId = 0
    AND ass.Assessment_Status in (4,6)
  INNER JOIN {{ ref("vwAssessmentDomain") }} AS ad
    ON ass.Assessment_ID = ad.AssessmentDomain_AssessmentId
    and ass.Assessment_TenantID = ad.AssessmentDomain_TenantId
  INNER JOIN QuestionAnswer AS qa
    ON ad.AssessmentDomain_ID = qa.Question_AssessmentDomainId
    and ad.AssessmentDomain_TenantID = qa.Question_TenantId
  LEFT JOIN QuestionGroupAnswers AS qga
    ON qa.Question_QuestionGroupResponseId = qga.QuestionGroupResponse_Id
    and qa.Question_TenantId = qga.QuestionGroupResponse_TenantId
  WHERE asst.Assessment_IsTemplate = 1
    and ass.Assessment_IsArchived = 0
)
, final as (
  SELECT DISTINCT
    Tenant_Id,
    Template_Name,
    Assessment_Name,
    ActualResults,
    QuestionType,
    IsGroupQuestion,
    MAX(Filter_UpdateTime)OVER (PARTITION BY Tenant_Id, Template_Name, Assessment_Name, ISNULL(ActualResults,''), QuestionType, IsGroupQuestion) AS Filter_UpdateTime
  FROM (
      select 
        Tenant_Id,
        Template_Name,
        Assessment_Name,
        ActualResults,
        QuestionType,
        IsGroupQuestion,
        Filter_UpdateTime
      from regular_question
      union 
      select 
        Tenant_Id,
        Template_Name,
        Assessment_Name,
        ActualResults,
        QuestionType,
        IsGroupQuestion,
        Filter_UpdateTime 
      from group_question
    )A
)

SELECT 
  Tenant_Id,
  Template_Name,
  Assessment_Name,
  ActualResults,
  QuestionType,
  IsGroupQuestion,
  Filter_UpdateTime
FROM final
{# where Tenant_Id = 1384 #}
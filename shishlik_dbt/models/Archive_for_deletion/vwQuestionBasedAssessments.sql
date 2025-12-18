{{ config(materialized="view") }}

with
    /*users(
  SELECT
        [Id] AS User_ID
        ,[Name]
        ,[NormalizedEmailAddress]
        ,[NormalizedUserName]
        ,[UserName]
        ,[Surname]
  FROM  {{ ref('vwAbpUser') }}
)


,*/
    Assessment as (
        select
            [Assessment_ID],
            [Assessment_TenantId],
            [Assessment_Name],
            [Assessment_Status],
            [Assessment_Objective],
            [Assessment_Description],
            [Assessment_Tags],
            [Assessment_Duedate]
        from {{ ref("vwAssessment") }}
    ),
    AssessmentDomain as (
        select
            [AssessmentDomain_ID],
            [AssessmentDomain_Name],
            [AssessmentDomain_AssessmentId],
            [AssessmentDomain_TenantId],
            [AssessmentDomain_IntroductionText],
            [AssessmentDomain_Order],
            [AssessmentDomain_PK]
        from {{ ref("vwAssessmentDomain") }}
    ),
    question_answer_flag as (
        select
            [Question_ID],
            [Question_Name],
            [Question_Description],
            [Question_Order],
            [Question_File],
            [Question_Type],
            [Question_AssessmentDomainId],
            [Question_TenantId],
            [Question_RiskStatus],
            [Answer_ID],
            [Answer_AssessmentResponseID],
            [Answer_RadioCustom],
            [Answer_Radio],
            [Answer_TextArea],
            [Answer_Submit],
            [Answer_MultiSelectValues],
            [Answer_JsonId],
            [Answer_MaxPossibleScore],
            [Answer_Score],
            [Answer_RiskStatus],
            [Answer_Compliance],
            [Answer_ResponderId],
            [Answer_ReviewerComment],
            [completed_flag]

        from {{ ref("vwQuestionAnswer") }}
    ),
    assessment_assessment_domain_join as (
        select

            dom. [AssessmentDomain_ID],
            dom. [AssessmentDomain_Name],
            dom. [AssessmentDomain_AssessmentId],
            dom. [AssessmentDomain_TenantId],
            dom. [AssessmentDomain_IntroductionText],
            dom. [AssessmentDomain_Order],
            asm. [Assessment_ID],
            asm. [Assessment_TenantId],
            asm. [Assessment_Name],
            asm. [Assessment_Status],
            asm. [Assessment_Objective],
            asm. [Assessment_Description],
            asm. [Assessment_Tags],
            asm. [Assessment_Duedate]
        from AssessmentDomain dom
        left join Assessment asm on dom.AssessmentDomain_AssessmentId = asm.Assessment_ID

    ),
    assessment_question_answer as (
        select
            assess. [AssessmentDomain_ID],
            assess. [AssessmentDomain_Name],
            assess. [AssessmentDomain_AssessmentId],
            assess. [AssessmentDomain_TenantId],
            assess. [AssessmentDomain_IntroductionText],
            assess. [AssessmentDomain_Order],
            assess. [Assessment_ID],
            assess. [Assessment_TenantId],
            assess. [Assessment_Name],
            assess. [Assessment_Status],
            assess. [Assessment_Objective],
            assess. [Assessment_Description],
            assess. [Assessment_Tags],
            assess. [Assessment_Duedate],
            quest. [Question_ID],
            quest. [Question_Name],
            quest. [Question_Description],
            quest. [Question_Order],
            quest. [Question_File],
            quest. [Question_Type],
            quest. [Question_AssessmentDomainId],
            quest. [Question_TenantId],
            quest. [Question_RiskStatus],
            quest. [Answer_ID],
            quest. [Answer_AssessmentResponseID],
            quest. [Answer_RadioCustom],
            quest. [Answer_Radio],
            quest. [Answer_TextArea],
            quest. [Answer_Submit],
            quest. [Answer_MultiSelectValues],
            quest. [Answer_JsonId],
            quest. [Answer_MaxPossibleScore],
            quest. [Answer_Score],
            quest. [Answer_RiskStatus],
            quest. [Answer_Compliance],
            quest. [Answer_ResponderId],
            quest. [Answer_ReviewerComment],
            quest. [completed_flag]
        from assessment_assessment_domain_join assess
        left join question_answer_flag quest on assess. [AssessmentDomain_ID] = quest. [Question_AssessmentDomainId]

    )

select *
from assessment_question_answer

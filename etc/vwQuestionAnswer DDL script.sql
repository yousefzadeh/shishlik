/****** Object:  View [reporting].[vwQuestionAnswer]    Script Date: 16/12/2025 11:28:12 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create view [reporting].[vwQuestionAnswer] as
    

with
    QUESTION as (
        select
            [Question_ID],
            [Question_Name],
            [Question_Description],
            [Question_Order],
            [Question_File],
            [Question_Type],
            [Question_TypeCode],
            [Question_AssessmentDomainId],
            [Question_ComponentStr],
            [Question_VendorDocumentRequired],
            [Question_Code],
            [Question_TenantId],
            [Question_Weighting],
            [Question_RiskStatus],
            [Question_Condition],
            [Question_HasConditionalLogic],
            [Question_IsVisibleIfConditional],
            [Question_HiddenInSurveyForConditional],
            [Question_DisplayDocumentUpload],
            [Question_QuestionGroupId],
            [Question_QuestionGroupResponseId],
            [Question_Suborder],
            [Question_RootQuestionId],
            [Question_ParentQuestionId],
            [Question_IsMandatory],
            [Question_IdRef],
            [Question_PK],
            [Question_UpdateTime],
            [Question_IsActive]
        from "sqldb-6clicks-staging"."reporting"."vwQuestionAll" qa
    ),
    ANSWER as (
        select
            [Answer_ID],
            [Answer_UpdateTime],
            [Answer_AssessmentResponseId],
            [Answer_QuestionId],
            [Answer_ComponentStr],
            [Answer_RadioCustom],
            [Answer_Radio],
            [Answer_TextArea],
            [Answer_Combined],
            [Answer_Explanation],
            [Answer_Submit],
            [Answer_MultiSelectValues],
            [Answer_JsonId],
            [Answer_Status],
            [Answer_TenantId],
            [Answer_MaxPossibleScore],
            [Answer_Score],
            [Answer_RiskStatus],
            [Answer_RiskStatusCode],
            [Answer_RiskStatusCalc],
            [Answer_Compliance],
            [Answer_ResponderId],
            [Answer_ReviewerComment],
            [Answer_PK]
        from "sqldb-6clicks-staging"."reporting"."vwAnswer"
    ),
    qa_choose_many as (
    select [Question_Id],
        [Answer_Id],
        [Answer_ResponseValue],
        [Answer_MaxPossibleScore],
        [Answer_Score],
        [Answer_ResponseCount]
        --,[AnswerChooseMany_UpdateTime]--not necessary to include this field. Dates from Answer module is already in other CTE 
     from "sqldb-6clicks-staging"."reporting"."vwAnswerChooseMany"
     ),
    question_answer_join as (
        select
            q. [Question_ID],
            q. [Question_Name],
            q. [Question_Description],
            greatest(q. [Question_UpdateTime],a. [Answer_UpdateTime]) UpdateTime,
            q. [Question_Order],
            q. [Question_File],
            q. [Question_Type],
            q. [Question_TypeCode],
            q. [Question_AssessmentDomainId],
            q. [Question_TenantId],
            q. [Question_RiskStatus],
            q. [Question_Weighting],
            q. [Question_ComponentStr],
            q. [Question_HiddenInSurveyForConditional],
            q. [Question_QuestionGroupId],
            q. [Question_QuestionGroupResponseId],
            q. [Question_IsActive],
            q. [Question_IdRef],
            a. [Answer_ID],
            a. [Answer_UpdateTime],
            a. [Answer_AssessmentResponseID],
            a. [Answer_ComponentStr],
            a. [Answer_RadioCustom],
            a. [Answer_Radio],
            a. [Answer_TextArea],
            a. [Answer_Combined],
            a. [Answer_Explanation],
            a. [Answer_Submit],
            a. [Answer_MultiSelectValues],
            a. [Answer_JsonId],
            a. [Answer_MaxPossibleScore],
            a. [Answer_Score],
            a. [Answer_RiskStatus],
            a. [Answer_RiskStatusCode],
            a. [Answer_RiskStatusCalc],
            a. [Answer_Compliance],
            a. [Answer_ResponderId],
            a. [Answer_ReviewerComment],
            a. [Answer_TenantId],
            case
                when q.Question_HiddenInSurveyForConditional = 1 and a.Answer_ResponderId is NULL
                then 'Skip Logic Applied'
                when a.Answer_Submit is NULL
                then 'Not Answered'
                else 'Responded'
            end
            [Question_Status]
        from QUESTION q
        left join Answer a on q.Question_ID = a.Answer_QuestionId and q.Question_TenantId = a.Answer_TenantId
    ),
    question_answer_flag as (
        select
            [Question_ID],
            [Question_Name],
            [Question_Description],
            [UpdateTime],
            [Question_Order],
            [Question_File],
            [Question_Type],
            [Question_TypeCode],
            [Question_AssessmentDomainId],
            Answer_TenantId AS [Question_TenantId],
            [Question_RiskStatus],
            [Question_Weighting],
            [Question_ComponentStr],
            [Question_Status],
            [Question_HiddenInSurveyForConditional],
            [Question_QuestionGroupId],
            [Question_QuestionGroupResponseId],
            [Question_IsActive],
            [Question_IdRef],
            [Answer_ID],
            [Answer_UpdateTime],
            [Answer_AssessmentResponseID],
            [Answer_ComponentStr],
            [Answer_RadioCustom],
            [Answer_Radio],
            [Answer_TextArea],
            [Answer_Combined],
            [Answer_Explanation],
            [Answer_Submit],
            [Answer_MultiSelectValues],
            case
                when Question_Type in (1)
                then [Answer_Radio]
                when Question_Type in (2, 5, 6, 10)
                then [Answer_RadioCustom]
                when Question_Type in (3, 7, 8)
                then [Answer_MultiSelectValues]
                when Question_Type in (4, 9)
                then [Answer_TextArea]
            end Answer_AnswerText,
            case when Question_Type in (4, 9) then '' else [Answer_TextArea] end Answer_AdditionalText,
            [Answer_JsonId],
            [Answer_MaxPossibleScore],
            [Answer_Score],
            [Answer_RiskStatus],
            [Answer_RiskStatusCode],
            [Answer_RiskStatusCalc],
            [Answer_Compliance],
            [Answer_ResponderId],
            [Answer_ReviewerComment],
            [Answer_TenantId],
            case
                when Question_Type in (1)
                then case when Answer_Radio is NULL then 0 else 1 end
                when Question_Type in (2, 5, 6, 10)  -- OR Question_Type = 5 OR Question_Type = 6 OR Question_Type = 10  --No data for 5-6 yet
                then case when Answer_RadioCustom is NULL or Answer_RadioCustom = '' then 0 else 1 end
                when Question_Type in (3, 7, 8)  -- OR Question_Type = 7 OR Question_Type = 8
                then case when Answer_MultiSelectValues is NULL or Answer_MultiSelectValues = '' then 0 else 1 end
                when Question_Type in (4, 9)  -- OR Question_Type = 9
                then case when Answer_TextArea is NULL or Answer_TextArea = '' then 0 else 1 end
                else 0
            end as completed_flag
        from question_answer_join
    ),
    -- , target_response as (
    -- SELECT 
    -- Question_Id
    -- ,json_value(kv.[value],'$.value') TargetResponseValue
    -- ,Question_Weighting * json_value(kv.[value],'$.rank') TargetResponseWeightedScore
    -- FROM QUESTION q
    -- cross apply openjson(Question_ComponentStr,'$.components.radiocustom.values') kv
    -- WHERE json_value(kv.[value],'$.isTargetResponse') = 'true'
    -- )
    final as (
        select distinct
            qa. [Question_ID],
            Question_IdRef,
            [Question_Name],
            [Question_Description],
            [Question_Order],
            [Question_File],
            [Question_Type],
            [Question_TypeCode],
            [Question_AssessmentDomainId],
            [Question_TenantId],
            [Question_RiskStatus],
            [Question_Weighting],
            [Question_ComponentStr],
            [Question_Status],
            [Question_HiddenInSurveyForConditional],
            [Question_QuestionGroupId],
            [Question_QuestionGroupResponseId],
            Question_IsActive,
            qa. [Answer_ID],
            -- [Answer_UpdateTime],
            [Answer_AssessmentResponseID],
            [Answer_ComponentStr],
            [Answer_RadioCustom],
            [Answer_Radio],
            [Answer_TextArea],
            [Answer_Combined],
            [Answer_Explanation],
            [Answer_Submit],
            Answer_TenantId,
            [Answer_MultiSelectValues],
            coalesce(qa. [Answer_AnswerText], qa2.Answer_ResponseValue)[Answer_AnswerText],
            [Answer_AdditionalText],
            [Answer_JsonId],
            -- ,[Answer_MaxPossibleScore]
            coalesce(qa. [Answer_MaxPossibleScore], qa2. [Answer_MaxPossibleScore])[Answer_MaxPossibleScore],
            coalesce(qa. [Answer_Score], qa2. [Answer_Score])[Answer_Score],
            -- ,[Answer_Score]
            [Answer_RiskStatus],
            [Answer_RiskStatusCode],
            [Answer_RiskStatusCalc],
            [Answer_Compliance],
            [Answer_ResponderId],
            [Answer_ReviewerComment],
            [completed_flag],
            coalesce(qa2.Answer_ResponseCount, 1) Answer_ResponseCount
        -- ,1 Answer_ResponseCount
        -- ,tr.TargetResponseValue
        -- ,tr.TargetResponseWeightedScore
			, greatest(qa. [UpdateTime],qa. [Answer_UpdateTime])  as QA_UpdateTime
        from question_answer_flag qa
        left join qa_choose_many qa2 
            on qa.Question_Id = qa2.Question_Id 
            and qa.Answer_Id = qa2.Answer_Id
    -- left join target_response tr on qa.Question_Id = tr.Question_Id
    )
select  *
from final
    
GO



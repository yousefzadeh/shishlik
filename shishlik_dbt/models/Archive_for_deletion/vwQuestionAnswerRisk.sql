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
            [Question_Suborder],
            [Question_RootQuestionId],
            [Question_ParentQuestionId],
            [Question_IsMandatory],
            [Question_IdRef],
            [Question_PK]
        from {{ ref("vwQuestion") }}
    ),
    ANSWER as (
        select
            [Answer_ID],
            [Answer_AssessmentResponseId],
            [Answer_QuestionId],
            [Answer_ComponentStr],
            [Answer_RadioCustom],
            [Answer_Radio],
            [Answer_TextArea],
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
        from {{ ref("vwAnswer") }}
    ),
    question_answer_join as (
        select
            q. [Question_ID],
            q. [Question_Name],
            q. [Question_Description],
            q. [Question_Order],
            q. [Question_File],
            q. [Question_Type],
            q. [Question_TypeCode],
            q. [Question_AssessmentDomainId],
            q. [Question_TenantId],
            q. [Question_RiskStatus],
            q. [Question_Weighting],
            q. [Question_ComponentStr],
            a. [Answer_ID],
            a. [Answer_AssessmentResponseID],
            a. [Answer_ComponentStr],
            a. [Answer_RadioCustom],
            a. [Answer_Radio],
            a. [Answer_TextArea],
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
            a. [Answer_ReviewerComment]
        from QUESTION q
        left join Answer a on q.Question_ID = a.Answer_QuestionId
    ),
    question_answer_flag as (
        select
            [Question_ID],
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
            [Answer_ID],
            [Answer_AssessmentResponseID],
            [Answer_ComponentStr],
            [Answer_RadioCustom],
            [Answer_Radio],
            [Answer_TextArea],
            [Answer_Submit],
            [Answer_MultiSelectValues],
            case
                [Question_TypeCode]
                when 'Yes No'
                then [Answer_Radio]
                when 'Choose One'
                then [Answer_RadioCustom]
                when 'Choose Many'
                then [Answer_MultiSelectValues]
                when 'Free Text Response'
                then [Answer_TextArea]
            end Answer_AnswerText,
            case [Question_TypeCode] when 'Free Text Response' then '' else [Answer_TextArea] end Answer_AdditionalText,
            [Answer_JsonId],
            [Answer_MaxPossibleScore],
            [Answer_Score],
            [Answer_RiskStatus],
            [Answer_RiskStatusCode],
            [Answer_RiskStatusCalc],
            [Answer_Compliance],
            [Answer_ResponderId],
            [Answer_ReviewerComment],
            case
                when Question_Type = 1
                then case when Answer_Radio is NULL then 0 else 1 end
                when Question_Type = 2 or Question_Type = 5 or Question_Type = 6 or Question_Type = 10  -- No data for 5-6 yet
                then case when Answer_RadioCustom is NULL or Answer_RadioCustom = '' then 0 else 1 end
                when Question_Type = 3 or Question_Type = 7 or Question_Type = 8
                then case when Answer_MultiSelectValues is NULL or Answer_MultiSelectValues = '' then 0 else 1 end
                when Question_Type = 4 or Question_Type = 9
                then case when Answer_TextArea is NULL or Answer_TextArea = '' then 0 else 1 end
                else 0
            end as completed_flag,
            ad.AssessmentDomain_Name
        from question_answer_join qah
        left join reporting.vwAssessmentDomain ad on ad.AssessmentDomain_ID = qah.Question_AssessmentDomainId
    ),
    min_max_avg as (
        select
            ad.AssessmentDomain_Name,
            ad.AssessmentDomain_Id,
            ad.AssessmentDomain_AssessmentId,
            sum(Answer_RiskStatusCalc) Total_Score,
            avg(Answer_RiskStatusCalc) Avg_Score
        from question_answer_join qaj
        join reporting.vwAssessmentDomain ad on ad.AssessmentDomain_Id = qaj.Question_AssessmentDomainId
        -- where Answer_Score is not null 
        -- and AssessmentDomain_Name = 'Finance & Legal'
        -- and AssessmentDomain_AssessmentId in (63, 620, 808)
        group by ad.AssessmentDomain_AssessmentId, ad.AssessmentDomain_Id, ad.AssessmentDomain_Name
    ),
    Total_Score as (
        select AssessmentDomain_Name, min(Total_Score) Min, max(Total_Score) Max, avg(Total_Score) Mean
        from min_max_avg
        -- where 
        -- AssessmentDomain_Name = 'Finance & Legal'
        -- AssessmentDomain_AssessmentId in (63, 620, 808)
        group by AssessmentDomain_Name
    ),
    Average_Score as (
        select AssessmentDomain_Name, min(Avg_Score) Min, max(Avg_Score) Max, avg(Avg_Score) Mean
        --
        from min_max_avg
        -- where 
        -- AssessmentDomain_Name = 'Finance & Legal'
        -- AssessmentDomain_AssessmentId in (63, 620, 808)
        group by AssessmentDomain_Name
    ),
    Total_Uni as (
        select qaf.*, mma.Min, null Max, null Mean, 'Min' Roll_Up
        --
        from question_answer_flag qaf
        left join Total_Score mma on mma.AssessmentDomain_Name = qaf.AssessmentDomain_Name
        -- where 
        -- mma.AssessmentDomain_Name = 'Finance & Legal'
        union all

        select qaf.*, null Min, mma.Max, null Mean, 'Max' Roll_Up
        from question_answer_flag qaf
        left join Total_Score mma on mma.AssessmentDomain_Name = qaf.AssessmentDomain_Name
        -- where 
        -- mma.AssessmentDomain_Name = 'Finance & Legal'
        union all

        select qaf.*, null Min, null Max, mma.Mean, 'Mean' Roll_Up
        from question_answer_flag qaf
        left join Total_Score mma on mma.AssessmentDomain_Name = qaf.AssessmentDomain_Name
    -- where 
    -- mma.AssessmentDomain_Name = 'Finance & Legal'
    ),
    -- --Average Score
    Average_Uni as (
        select qaf.*, mma.Min, null Max, null Mean, 'Min' Roll_Up
        --
        from question_answer_flag qaf
        left join Average_Score mma on mma.AssessmentDomain_Name = qaf.AssessmentDomain_Name
        -- where 
        -- mma.AssessmentDomain_Name = 'Finance & Legal'
        union all

        select qaf.*, null Min, mma.Max, null Mean, 'Max' Roll_Up
        --
        from question_answer_flag qaf
        left join Average_Score mma on mma.AssessmentDomain_Name = qaf.AssessmentDomain_Name
        -- where 
        -- mma.AssessmentDomain_Name = 'Finance & Legal'
        union all

        select qaf.*, null Min, null Max, mma.Mean, 'Mean' Roll_Up
        --
        from question_answer_flag qaf
        left join Average_Score mma on mma.AssessmentDomain_Name = qaf.AssessmentDomain_Name
    -- where 
    -- mma.AssessmentDomain_Name = 'Finance & Legal'
    ),
    -- --main query
    Methodology as (
        select 'Total' Methodology, Total_Uni.*
        from Total_Uni
        -- where Roll_Up = 'Mean'
        union all

        select 'Average' Methodology, Average_Uni.*
        from Average_Uni
    )
select *
from
    Methodology m
    -- where Roll_Up = 'Mean' and AssessmentDomain_Name = 'Organisational context'
    

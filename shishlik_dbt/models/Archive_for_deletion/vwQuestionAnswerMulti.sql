/************* 

    Answers for all option choices

    Question table contains the option choices in the ComponentStr

 **************/
select
    Question_Id,
    Question_OptionId,
    Question_AssessmentDomainId,
    Question_Type,
    case
        when [Question_Type] = 1
        then 'Yes No'
        when [Question_Type] in (2, 5, 6, 10)
        then 'Choose One'
        when [Question_Type] in (3, 7, 8)
        then 'Choose Many'
        when [Question_Type] in (4, 9)
        then 'Free Text Response'
        else 'Undefined'
    end as Question_TypeCode,
    Answer_Id,
    Answer_ChoiceId,
    Answer_AssessmentResponseId,
    Answer_QuestionId,
    Answer_ComponentStr,
    Answer_Status,
    Answer_TenantId,
    Answer_MaxPossibleScore,
    Answer_AnswerText,
    /*********** 
        The weighted score of a question will be (question weighting * selected answer multiplier)
        The weighting column is from question table
        The selected answer multipler is the 'rank' attribute of the selected choice (question componentstr)

        Question_OptionXXX are extracted from ComponentStr of Question Table
        The expanded questions and all choices are done in vwAnswerChoices
     ************/
    qc.Question_Weighting,  -- Question_Id grain
    qc.Question_OptionRank Question_Multiplier,  -- Question_Id/ChoiceId grain
    /************ 
        For Risk Rated roll ups, use the RiskStatusCalc to aggregate min, max, average
        For Weighted Score roll ups, use the WeightedScore to aggregate min, max, average
    ************/
    qc.Question_Weighting * qc.Question_OptionRank Answer_WeightedScore,
    qc.Question_OptionRiskStatus Answer_RiskStatus,
    case
        when qc.Question_OptionRiskStatus = 0
        then 'No Risk'
        when qc.Question_OptionRiskStatus = 6
        then 'Very Low'
        when qc.Question_OptionRiskStatus = 1
        then 'Low'
        when qc.Question_OptionRiskStatus = 3
        then 'Medium'
        when qc.Question_OptionRiskStatus = 4
        then 'High'
        when qc.Question_OptionRiskStatus = 5
        then 'Very High'
    end as Answer_RiskStatusCode,
    case
        when qc.Question_OptionRiskStatus = 5
        then 5
        when qc.Question_OptionRiskStatus = 4
        then 4
        when qc.Question_OptionRiskStatus = 3
        then 3
        when qc.Question_OptionRiskStatus = 1
        then 2
        when qc.Question_OptionRiskStatus = 6
        then 1
        when qc.Question_OptionRiskStatus = 0
        then 0
    end as Answer_RiskStatusCalc,
    Answer_Compliance,
    Answer_ResponderId,
    Answer_ReviewerComment,
    -- Answer_AnswerText = '' matches a space padded empty string all spaces = '' SQL 92 standard
    case when Answer_AnswerText = '' then 0 else 1 end Answer_IsCompleted
from {{ ref("vwQuestionChoices") }} qc
left join
    {{ ref("vwAnswerSelected") }} a on qc.Question_Id = a.Answer_QuestionId and qc.Question_OptionId = a.Answer_ChoiceId
    -- Question attribute value.value to join with answer. 
    

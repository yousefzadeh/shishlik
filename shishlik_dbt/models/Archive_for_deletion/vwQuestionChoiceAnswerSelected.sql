/************* 

    One row = 1 Question and 1 Choice and 1 Answer and 1 Option

    - Question table contains the option choices in the ComponentStr
    - Answers for all option choices in single answer and multiple answer question types
    - One question can have several answers as it is attempted multiple times


 **************/
select
    qc.Question_Id,
    qc.Question_OptionId,
    qc.Question_AssessmentDomainId,
    qc.Question_Type,
    case
        when qc. [Question_Type] = 1
        then 'Yes No'
        when qc. [Question_Type] in (2, 5, 6, 10)
        then 'Choose One'
        when qc. [Question_Type] in (3, 7, 8)
        then 'Choose Many'
        when qc. [Question_Type] in (4, 9)
        then 'Free Text Response'
        else 'Undefined'
    end as Question_TypeCode,
    a.Answer_Id,
    a.Answer_ChoiceId,
    a.Answer_AssessmentResponseId,
    a.Answer_QuestionId,
    a.Answer_ComponentStr,
    a.Answer_Status,
    a.Answer_TenantId,
    a.Answer_MaxPossibleScore,
    /*********** 
        The weighted score of a question will be (question weighting * selected answer multiplier)
        The weighting column is from question table
        The selected answer multipler is the 'rank' attribute of the selected choice (question componentstr)

        Question_OptionXXX are extracted from ComponentStr of Question Table
        The expanded questions and all choices are done in vwAnswerChoices
     ************/
    qc.Question_Weighting,  -- Question_Id grain
    qc.Question_OptionRank QuestionChoice_Multiplier,  -- Question_Id/ChoiceId grain
    qc.Question_Weighting * qc.Question_OptionRank QuestionChoice_WeightedScore,  -- Question_Id/ChoiceId grain
    /************ 
        For Risk Rated roll ups, use the RiskStatusCalc to aggregate min, max, average
        For Weighted Score roll ups, use the WeightedScore to aggregate min, max, average
    ************/
    a.Answer_AnswerText AnswerSelected_AnswerText,
    -- Answer_AnswerText = '' matches a space padded empty string all spaces = '' SQL 92 standard
    case when Answer_AnswerText = '' then 0 else 1 end AnswerSelected_IsCompleted,
    case
        when a.Answer_AnswerText <> '' then qc.Question_Weighting * qc.Question_OptionRank
    end AnswerSelected_WeightedScore,
    qc.Question_OptionRiskStatus AnswerSelected_RiskStatus,
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
    end as AnswerSelected_RiskStatusCode,
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
    end as AnswerSelected_RiskStatusCalc,
    Answer_Compliance,
    Answer_ResponderId,
    Answer_ReviewerComment,
    Answer_Version,
    Answer_IsCurrent
from {{ ref("vwQuestionChoices") }} qc
left join
    {{ ref("vwAnswerSelected") }} a on qc.Question_Id = a.Answer_QuestionId and qc.Question_OptionId = a.Answer_ChoiceId
    -- Question attribute value.value to join with answer. 
    

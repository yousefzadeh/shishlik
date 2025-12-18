/***********

    One row is a selected option answered
    Rows are expanded from ComponentStr

    The selected choice (AnswerText) is a String Value that is looked up to the Question Choices to get the RiskStatus or WeightedScore

    2 types of ComponentStr to consider to extract the AnswerText
    - Choose One    in select clause - json_value(ComponentStr,'$.RadioCustom') 
    - Choose Many   
        generate multiple rows - cross apply openjson(ComponentStr,'$.MultiSelectValues')
        select clause - value (value) of the attribute (key index 0..) 
 
    Yes No questions are excluded
    Free Text Response is not scored

    Answer_AnswerText needs to be joined to question componentstr attribute "value" to get the score

 ***********/
with
    ans as (
        select
            Answer_Id,
            Answer_AssessmentResponseId,
            Answer_QuestionId,
            Answer_ComponentStr,
            Answer_Status,
            Answer_TenantId,
            Answer_MaxPossibleScore,
            Answer_Combined,
            Answer_MultiSelectValues,
            Answer_Compliance,
            Answer_ResponderId,
            Answer_ReviewerComment,
            Answer_Version,
            Answer_IsCurrent
        from {{ ref("vwAnswer") }}
    ),
    not_multi as (
        -- Choose one
        -- single answer in RadioCustom
        select
            Answer_Id,
            1 Answer_ChoiceId,
            Answer_AssessmentResponseId,
            Answer_QuestionId,
            Answer_ComponentStr,
            Answer_Status,
            Answer_TenantId,
            Answer_MaxPossibleScore,
            cast(Answer_Combined as nvarchar(4000)) Answer_AnswerText,
            Answer_Compliance,
            Answer_ResponderId,
            Answer_ReviewerComment,
            Answer_Version,
            Answer_IsCurrent
        from ans a
        where Answer_MultiSelectValues is NULL
    ),
    multi as (
        -- Choose many
        -- many answers in multiselect
        select
            Answer_Id,
            cast(kv. [key] as int) Answer_ChoiceId,
            Answer_AssessmentResponseId,
            Answer_QuestionId,
            Answer_ComponentStr,
            Answer_Status,
            Answer_TenantId,
            Answer_MaxPossibleScore,
            cast(kv. [value] as nvarchar(4000)) Answer_AnswerText,
            Answer_Compliance,
            Answer_ResponderId,
            Answer_ReviewerComment,
            Answer_Version,
            Answer_IsCurrent
        from ans a cross apply openjson(Answer_ComponentStr, '$.MultiSelectValues') kv
        where Answer_MultiSelectValues is not null
    ),
    all_cases as (
        select *
        from not_multi
        union
        select *
        from multi
    )
select distinct a.*
from all_cases a

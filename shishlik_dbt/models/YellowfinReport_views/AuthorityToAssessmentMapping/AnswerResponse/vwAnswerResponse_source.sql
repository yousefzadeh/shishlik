{{ config(materialized="view") -}}
with
    a_raw as (
        select
            -- system generated fields from app macro
            [Id],
            [CreationTime],
            [CreatorUserId],
            [LastModificationTime],
            [LastModifierUserId],
            [IsDeleted],
            [DeleterUserId],
            [DeletionTime],
            [AssessmentResponseId],
            [QuestionId],
            [ComponentStr] ComponentStr,
            cast(JSON_VALUE(ComponentStr, '$.Submit') as nvarchar(100)) as Submit,
            cast(JSON_VALUE(ComponentStr, '$.Id') as nvarchar(200)) as JsonId,
            [Status],
            [TenantId],
            [MaxPossibleScore],
            [Score],
            [RiskStatus],
            case
                when [RiskStatus] = 0
                then 'No Risk'
                when [RiskStatus] = 6
                then 'Very Low Risk'
                when [RiskStatus] = 1
                then 'Low Risk'
                when [RiskStatus] = 3
                then 'Medium Risk'
                when [RiskStatus] = 4
                then 'High Risk'
                when [RiskStatus] = 5
                then 'Very High Risk'
                else 'Undefined'
            end as [RiskStatusCode],
            case
                when [RiskStatus] = 5
                then 5.0
                when [RiskStatus] = 4
                then 4.0
                when [RiskStatus] = 3
                then 3.0
                when [RiskStatus] = 1
                then 2.0
                when [RiskStatus] = 6
                then 1.0
                when [RiskStatus] = 0
                then 0.0
                else NULL
            end as [RiskStatusCalc],
            [Compliance],
            [ResponderId],
            cast([ReviewerComment] as nvarchar(1000)) as [ReviewerComment],
            row_number() over (partition by QuestionId order by Id)[Version],
            case when row_number() over (partition by QuestionId order by Id desc) = 1 then 1 else 0 end IsCurrent,
            {# ,UpdateTime -- uncomment this when deploying tables and after creating the UpdateTime calculated column -#}
            CONCAT(AssessmentResponseId, QuestionId, tenantID) as PK
        from {{ source("assessment_models", "Answer") }} a
        where IsDeleted = 0
    ),
    a as (
        select
            [ID] as [Answer_ID],
            [AssessmentResponseId] as [Answer_AssessmentResponseId],
            [QuestionId] as [Answer_QuestionId],
            [ComponentStr] as [Answer_ComponentStr],
            [Submit] as [Answer_Submit],
            [JsonId] as [Answer_JsonId],
            [Status] as [Answer_Status],
            [TenantId] as [Answer_TenantId],
            [MaxPossibleScore] as [Answer_MaxPossibleScore],
            [Score] as [Answer_Score],
            [RiskStatus] as [Answer_RiskStatus],
            [RiskStatusCalc] as [Answer_RiskStatusCalc],
            [RiskStatusCode] as [Answer_RiskStatusCode],
            [Compliance] as [Answer_Compliance],
            [ResponderId] as [Answer_ResponderId],
            [ReviewerComment] as [Answer_ReviewerComment],
            [Version] as [Answer_Version],
            [IsCurrent] as [Answer_IsCurrent],
            {# [UpdateTime] AS [Answer_UpdateTime], #}
            [PK] as [Answer_PK]
        from a_raw
    ),
    q_raw as (  -- dbo.Question with casting

        {# 
    Question.Type in (3,7,8) for "Choose Many" type of questions
    Question.Type not in (3,7,8) for "Choose Many" type of questions

    Performance tuning:
    * WHERE Type in (3,7,8) will cause inconsistent plans because of of EQUALITY ARRAY operator
    * WHERE Type = 3 UNION ALL WHERE Type = 7 UNION ALL WHERE Type = 8
        - Consistent query plans because we changed the EQUALITY ARRAY into EQUALITY SCALAR operator
        - For choose many loop 3 times, except choose many will loop 7 times, total of always taking 10 loops
        - Loop 10 times in all cases
    * Create calculated column IsMultiSelectType and index it
        - WHERE IsMultiSelectType = 0 and WHERE IsMultiSelectType = 1 are both EQUALITY SCALAR
        - Consistent plans, single loop
        - Most efficient
#}
        select
            q.TenantId Question_TenantId,
            q.id Question_Id,
            q.IdRef Question_IdRef,
            q.Name Question_Name,
            q.AssessmentDomainId Question_AssessmentDomainId,
            Type Question_Type,
            case when Type in (3, 7, 8) then 1 else 0 end Question_IsMultiSelectType,
            q.Weighting Question_Weighting,
            ComponentStr Question_ComponentStr
        {# ,
    UpdateTime Question_UpdateTime #}
        from {{ source("assessment_models", "Question") }} q
    ),
    target_response as (
        select
            Question_Id,
            json_value(kv. [value], '$.value') TargetResponseValue,
            Question_Weighting * json_value(kv. [value], '$.rank') TargetResponseWeightedScore
        from q_raw cross apply openjson(Question_ComponentStr, '$.components.radiocustom.values') kv
        where json_value(kv. [value], '$.isTargetResponse') = 'true'
    ),
    q_typecode as (  -- Tidy question option json based on question type
        select
            q.Question_TenantId,
            q.Question_Id,
            q.Question_IdRef,
            q.Question_Name,
            q.Question_AssessmentDomainId,
            q.Question_Type,
            Question_IsMultiSelectType,
            case
                when Question_Type = 1
                then 'Yes No'
                when Question_Type in (2, 5, 6, 10)
                then 'Choose One'
                when Question_Type in (3, 7, 8)
                then 'Choose Many'
                when Question_Type in (4, 9)
                then 'Free Text Response'
                else 'Undefined'
            end as [Question_TypeCode]
        {# ,
    q.Question_UpdateTime #}
        from q_raw q
    ),
    q as (  -- wrap the option string in json array 
        select
            q.Question_TenantId,
            q.Question_Id,
            q.Question_IdRef,
            q.Question_Name,
            q.Question_AssessmentDomainId,
            q.Question_Type,
            q.Question_IsMultiSelectType,
            q.Question_TypeCode
        {# ,
    q.Question_UpdateTime #}
        from q_typecode q
    ),
    qa_single as (
        -- join question to answer for single row response type
        select
            Question_TenantId,
            Question_Id,
            Question_IdRef,
            Question_Name,
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Answer_QuestionId,
            Answer_Id,
            Answer_Version,
            Answer_IsCurrent,
            -- -
            Answer_AssessmentResponseID,
            Answer_Submit,
            Answer_JsonId,
            Answer_Compliance,
            Answer_ResponderId,
            Answer_ReviewerComment,
            1 Answer_ResponseCount,
            -- -
            Answer_Score,
            Answer_RiskStatus,
            Answer_RiskStatusCode,
            Answer_RiskStatusCalc,
            Answer_MaxPossibleScore,
            0 AnswerResponse_Key,
            -- cast to shorter string to reduce IO and memory -- dev max len 93
            cast(
                case
                    when Question_Type in (1)
                    then coalesce(json_value(a.Answer_ComponentStr, '$.Radio'), 'Blank')
                    when Question_Type in (2, 5, 6, 10)
                    then coalesce(json_value(a.Answer_ComponentStr, '$.RadioCustom'), 'Blank')
                    when Question_Type in (4, 9)
                    then 'Free Text'  -- Match the value in Question
                end as nvarchar(400)
            ) AnswerResponse_Value,
            json_value(a.Answer_ComponentStr, '$.TextArea') Answer_TextArea,

            {# Answer_UpdateTime, #}
            cast(a.Answer_Id as varchar(10)) + '_0' AnswerResponse_PK
        from q
        join a on q.Question_Id = a.Answer_QuestionId
        where Question_IsMultiSelectType = 0
    ),
    qa_multi_json as (
        -- join question to answer for multi response type
        -- tidy answer json based on question type.
        -- prepare answer json as an array for cross apply openjson()
        select
            Question_TenantId,
            Question_Id,
            Question_IdRef,
            Question_Name,
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Answer_QuestionId,
            Answer_Id,
            Answer_Version,
            Answer_IsCurrent,
            -- -
            Answer_AssessmentResponseID,
            Answer_Submit,
            Answer_JsonId,
            Answer_Compliance,
            Answer_ResponderId,
            Answer_ReviewerComment,
            1 Answer_ResponseCount,
            -- -
            Answer_Score,
            Answer_RiskStatus,
            Answer_RiskStatusCode,
            Answer_RiskStatusCalc,
            Answer_MaxPossibleScore,
            '{ "answer" : ' + json_query(a.Answer_ComponentStr, '$.MultiSelectValues') + ' }' Answer_ResponseJson,
            -- json array for cross apply openjson()
            json_value(a.Answer_ComponentStr, '$.TextArea') Answer_TextArea
        {# ,
    Answer_UpdateTime #}
        from q
        join a on q.Question_Id = a.Answer_QuestionId
        where Question_IsMultiSelectType = 1
    ),
    qa_multi as (
        select
            Question_TenantId,
            Question_Id,
            Question_IdRef,
            Question_Name,
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Answer_QuestionId,
            Answer_Id,
            Answer_Version,
            Answer_IsCurrent,
            -- -
            Answer_AssessmentResponseID,
            Answer_Submit,
            Answer_JsonId,
            Answer_Compliance,
            Answer_ResponderId,
            Answer_ReviewerComment,
            Answer_ResponseCount,
            -- -
            Answer_Score,  -- NULL
            Answer_RiskStatus,
            Answer_RiskStatusCode,
            Answer_RiskStatusCalc,
            Answer_MaxPossibleScore,  -- NULL
            cast(answer_kv. [key] + 1 as int) AnswerResponse_Key,
            -- reduce string length to reduce IO and memory
            cast(answer_kv. [value] as nvarchar(400)) AnswerResponse_Value,
            Answer_TextArea,
            {# Answer_UpdateTime, #}
            cast(qa.Answer_Id as varchar(10)) + '_' + answer_kv. [key] AnswerResponse_PK
        from qa_multi_json qa outer apply openjson(qa.Answer_ResponseJson, '$.answer') answer_kv
    ),
    qa as (
        select *
        from qa_single
        union all
        select *
        from qa_multi
    ),
    null_answer as (
        -- Add row for Null Answers
        select
            0 Answer_TenantId,
            0 Answer_Id,
            0 Answer_QuestionId,
            '0' Question_IdRef,
            'No Name' Question_Name,
            0 Question_AssessmentDomainId,
            '1' Question_Type,
            0 Answer_Version,
            0 Answer_IsCurrent,
            -- -
            0 Answer_AssessmentResponseID,
            '' Answer_Submit,
            '0' Answer_JsonId,
            '' Answer_Compliance,
            0 Answer_ResponderId,
            '' Answer_ReviewerComment,
            1 completed_flag,
            1 Answer_ResponseCount,
            -- -
            0 Answer_Score,
            0 Answer_RiskStatus,
            'No Response Attempted' Answer_RiskStatusCode,
            0 Answer_RiskStatusCalc,
            0 Answer_MaxPossibleScore,
            'No Response' Answer_TextArea,
            0 AnswerResponse_Key,
            'Not Attempted' AnswerResponse_Value,
            {# cast('2000-01-01' as datetime) Answer_UpdateTime, #}
            '0_0' AnswerResponse_PK
    ),
    aa as (  -- multi row answers
        select
            Question_TenantId Answer_TenantId,
            Answer_Id,
            Answer_QuestionId,
            Question_IdRef,
            Question_Name,
            Question_AssessmentDomainId,
            Question_Type,
            Answer_Version,
            Answer_IsCurrent,
            -- -
            Answer_AssessmentResponseID,
            Answer_Submit,
            Answer_JsonId,
            Answer_Compliance,
            Answer_ResponderId,
            Answer_ReviewerComment,
            case
                when AnswerResponse_Value is NULL or AnswerResponse_Value in ('', 'Blank', 'Not Attempted')
                then 0
                else 1
            end as completed_flag,
            Answer_ResponseCount,
            -- -
            Answer_Score,
            Answer_RiskStatus,
            Answer_RiskStatusCode,
            Answer_RiskStatusCalc,
            Answer_MaxPossibleScore,
            Answer_TextArea,
            AnswerResponse_Key,
            AnswerResponse_Value,
            {# Answer_UpdateTime, #}
            AnswerResponse_PK
        from qa
    ),
    final as (
        select *
        from aa
        union all
        select *
        from null_answer
    )
select final.*, tr.TargetResponseValue, tr.TargetResponseWeightedScore
from final
left join target_response tr on final.Answer_QuestionId = tr.Question_Id

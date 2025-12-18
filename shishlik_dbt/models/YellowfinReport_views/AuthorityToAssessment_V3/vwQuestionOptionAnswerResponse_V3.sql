{{ config(materialized="view") -}}
with
    a_raw as (
        select
            Id,
            QuestionId,
             case when ComponentStr = '' then NULL else  ComponentStr 
                    end as ComponentStr,
            TenantId,
            -- Score, RiskStatus, MaxPossibleScore is correct for single answer questions
            -- Need to calculate from unnested json for multi-answer questions 
            MaxPossibleScore,
            Score,
            RiskStatus,
            case
                RiskStatus
                when 0
                then 'No Risk'
                when 6
                then 'Very Low Risk'
                when 1
                then 'Low Risk'
                when 3
                then 'Medium Risk'
                when 4
                then 'High Risk'
                when 5
                then 'Very High Risk'
                else 'Undefined'
            end as RiskStatusCode,
            case
                RiskStatus
                when 0
                then 0.0
                when 6
                then 1.0
                when 1
                then 2.0
                when 3
                then 3.0
                when 4
                then 4.0
                when 5
                then 5.0
                else NULL
            end as RiskStatusCalc,
            --
            Compliance,
            ResponderId,
            AssessmentResponseId
        from {{ source("assessment_models", "Answer") }} a
        where IsDeleted = 0 and [Status] = 3
    ),
    a as (
        select
            ID as Answer_ID,
            QuestionId as Answer_QuestionId,
            COALESCE(
                ComponentStr,
                '{"RadioCustom":null,"Radio":null,"TextArea":null,"Submit":false,"MultiSelectValues":null,"Id":0}'
            ) as Answer_ComponentStr,
            TenantId as Answer_TenantId,
            MaxPossibleScore as Answer_MaxPossibleScore,
            Score as Answer_Score,
            RiskStatus as Answer_RiskStatus,
            RiskStatusCalc as Answer_RiskStatusCalc,
            RiskStatusCode as Answer_RiskStatusCode,
            Compliance as Answer_Compliance,
            JSON_VALUE(ComponentStr, '$.Submit') as Answer_Submit,
            ResponderId as Answer_ResponderId,
            AssessmentResponseId as Answer_AssessmentResponseId
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
            {# If IsMultiSelect column is created then use IsMultiSelectType column in this expression 
    IsMultiSlecetType as Question_IsMultiSelectType, #}
            case when Type in (3, 7, 8) then 1 else 0 end Question_IsMultiSelectType,
            q.Weighting Question_Weighting,
            json_modify(
                case
                    when Type in (2, 5, 6, 10)
                    then json_query(q.ComponentStr, '$.components.radiocustom.values')
                    when Type in (3, 7, 8)
                    then json_query(q.ComponentStr, '$.components.multiselect.values')
                end,
                'append $',
                json_query('{ "value": "NULL" }')
            ) Question_OptionJson,
            HiddenInSurveyForConditional Question_HiddenInSurveyForConditional,
            QuestionGroupId Question_QuestionGroupId
        from {{ source("assessment_models", "Question") }} q
    ),
    q as (  -- Tidy question option json based on question type
        select
            q.Question_TenantId,
            q.Question_Id,
            q.Question_IdRef,
            q.Question_Name,
            q.Question_AssessmentDomainId,
            q.Question_Type,
            q.Question_IsMultiSelectType,
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
            end as Question_TypeCode,
            Question_Weighting,
            Question_OptionJson,
            Question_HiddenInSurveyForConditional,
            Question_QuestionGroupId
        from q_raw q
    ),
    qq as (  -- unnest question to get Rank and weighting to calculate score and max possible scoree
        select q.Question_Id, q.Question_Weighting, QuestionOption_Value, QuestionOption_Rank, QuestionOption_RiskStatus
        from q outer apply openjson(Question_OptionJson, '$')
        with
            (
                QuestionOption_Value nvarchar(4000) '$.value',
                QuestionOption_Rank INT '$.rank',
                QuestionOption_RiskStatus INT '$.riskStatus'
            ) question_kv
        where q.Question_Type in (3, 7, 8)
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
            Question_Weighting,
            Question_HiddenInSurveyForConditional,
            Question_QuestionGroupId,
            Answer_QuestionId,
            Answer_Id,
            Answer_Compliance,
            1 Answer_ResponseCount,
            -- -
            Answer_Score,
            Answer_RiskStatus,
            Answer_RiskStatusCode,
            Answer_RiskStatusCalc,
            Answer_MaxPossibleScore,
            '{ "answer" : ' + json_query(a.Answer_ComponentStr, '$.MultiSelectValues') + ' }' Answer_ResponseJson,
            -- json array for cross apply openjson()
            json_value(a.Answer_ComponentStr, '$.TextArea') Answer_TextArea,
            Answer_Submit,
            Answer_ResponderId,
            Answer_AssessmentResponseId
        from q
        join a on q.Question_Id = a.Answer_QuestionId
        where Question_IsMultiSelectType = 1 and a.Answer_ComponentStr is not NULL
    ),
    -- - Start of union tables
    qa_no_answer as (
        -- Question with no answer - score is zero
        select
            Question_TenantId,
            Question_Id,
            Question_IdRef,
            Question_Name,
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Question_Weighting,
            Question_HiddenInSurveyForConditional,
            Question_QuestionGroupId,
            Question_Id Answer_QuestionId,
            0 Answer_Id,
            NULL Answer_Compliance,
            0 Answer_ResponseCount,
            0 Answer_Score,
            NULL Answer_RiskStatus,
            NULL Answer_RiskStatusCode,
            NULL Answer_RiskStatusCalc,
            NULL Answer_MaxPossibleScore,
            NULL AnswerResponse_key,
            'Not Responded' AnswerResponse_Value,
            'Not Responded' Answer_TextArea,
            NULL Answer_Submit,
            NULL Answer_ResponderId,
            NULL Answer_AssessmentResponseId,
            '0_0' AnswerResponse_PK
        from q
        left join a on q.Question_Id = a.Answer_QuestionId
        where Answer_Id is NULL
    ),
    qa_freetext as (
        -- Question with Answer Type is Free text 4,9, score = NULL
        select
            Question_TenantId,
            Question_Id,
            Question_IdRef,
            Question_Name,
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Question_Weighting,
            Question_HiddenInSurveyForConditional,
            Question_QuestionGroupId,
            Answer_QuestionId,
            Answer_Id,
            Answer_Compliance,
            1 Answer_ResponseCount,
            NULL Answer_Score,
            Answer_RiskStatus,
            Answer_RiskStatusCode,
            Answer_RiskStatusCalc,
            Answer_MaxPossibleScore,
            0 AnswerResponse_key,
            cast(
                case
                    when json_value(a.Answer_ComponentStr, '$.TextArea') = ''
                    then 'Blank'
                    else json_value(a.Answer_ComponentStr, '$.TextArea')
                end as nvarchar(400)
            ) AnswerResponse_Value,
            json_value(a.Answer_ComponentStr, '$.TextArea') Answer_TextArea,
            Answer_Submit,
            Answer_ResponderId,
            Answer_AssessmentResponseId,
            cast(a.Answer_Id as varchar(10)) + '_0' AnswerResponse_PK
        from q
        join a on q.Question_Id = a.Answer_QuestionId
        where Question_Type in (4, 9)
    ),
    qa_single as (
        -- Question with Answer of Single Answer type
        select
            Question_TenantId,
            Question_Id,
            Question_IdRef,
            Question_Name,
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Question_Weighting,
            Question_HiddenInSurveyForConditional,
            Question_QuestionGroupId,
            Answer_QuestionId,
            Answer_Id,
            Answer_Compliance,
            1 Answer_ResponseCount,
            Answer_Score,
            Answer_RiskStatus,
            Answer_RiskStatusCode,
            Answer_RiskStatusCalc,
            Answer_MaxPossibleScore,
            0 AnswerResponse_key,
            cast(
                case
                    when Question_Type in (1)
                    then json_value(a.Answer_ComponentStr, '$.Radio')
                    when Question_Type in (2, 5, 6, 10)
                    then json_value(a.Answer_ComponentStr, '$.RadioCustom')
                end as nvarchar(400)
            ) AnswerResponse_Value,
            json_value(a.Answer_ComponentStr, '$.TextArea') Answer_TextArea,
            Answer_Submit,
            Answer_ResponderId,
            Answer_AssessmentResponseId,
            cast(a.Answer_Id as varchar(10)) + '_0' AnswerResponse_PK
        from q
        join a on q.Question_Id = a.Answer_QuestionId
        where Question_IsMultiSelectType = 0 and a.Answer_ComponentStr is not NULL and Question_Type in (1, 2, 5, 6, 10)
    ),
    qa_multi as (
        select
            qa.Question_TenantId,
            qa.Question_Id,
            qa.Question_IdRef,
            qa.Question_Name,
            qa.Question_AssessmentDomainId,
            qa.Question_Type,
            qa.Question_TypeCode,
            qa.Question_Weighting,
            qa.Question_HiddenInSurveyForConditional,
            qa.Question_QuestionGroupId,
            qa.Answer_QuestionId,
            qa.Answer_Id,
            qa.Answer_Compliance,
            qa.Answer_ResponseCount,
            cast(qq.Question_Weighting * qq.QuestionOption_Rank as decimal(15, 2)) Answer_Score,
            qq.QuestionOption_RiskStatus Answer_RiskStatus,
            case
                qq.QuestionOption_RiskStatus
                when 0
                then 'No Risk'
                when 6
                then 'Very Low Risk'
                when 1
                then 'Low Risk'
                when 3
                then 'Medium Risk'
                when 4
                then 'High Risk'
                when 5
                then 'Very High Risk'
                else 'Undefined'
            end as RiskStatusCode,
            case
                qq.QuestionOption_RiskStatus
                when 0
                then 0.0
                when 6
                then 1.0
                when 1
                then 2.0
                when 3
                then 3.0
                when 4
                then 4.0
                when 5
                then 5.0
                else NULL
            end as RiskStatusCalc,
            sum(CAST(qq.Question_Weighting as bigint) * CAST(qq.QuestionOption_Rank as bigint)) over (
                partition by qq.Question_Id
            ) Question_MaxPossibleScore,
            cast(answer_kv. [key] + 1 as int) AnswerResponse_key,
            answer_kv.value AnswerResponse_Value,
            qa.Answer_TextArea,
            Answer_Submit,
            Answer_ResponderId,
            Answer_AssessmentResponseId,
            concat(qa.Answer_Id, '_', answer_kv. [key]) AnswerResponse_PK
        from qa_multi_json qa outer apply openjson(qa.Answer_ResponseJson, '$.answer') as answer_kv
        left join qq on qa.Question_Id = qq.Question_Id and answer_kv.value = qq.QuestionOption_Value
    ),
    qa as (
        select 'single' part, *
        from qa_single  -- Question with answer of type single answer
        union all
        select 'multi' part, *
        from qa_multi  -- Question with answer of type Multi Select Answer
        union all
        select 'freetext' part, *
        from qa_freetext  -- Question of freetext type
        union all
        select 'no answer' part, *
        from qa_no_answer  -- Question with no answer
    ),
    final as (  -- multi row answers
        select Distinct
            part,
            Question_TenantId Answer_TenantId,
            Question_Id,
            Question_IdRef,
            Question_Name,
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Question_Weighting,
            Answer_Id,
            Answer_Compliance,
            Answer_ResponseCount,
            Answer_MaxPossibleScore,
            Answer_ResponderId,
            Answer_AssessmentResponseId,
            AnswerResponse_key,
            -- Actual Response is used in group by in count chart, cannot be NULL    
            case
                when Question_HiddenInSurveyForConditional = 1
                then 'Blank'
                when Question_Type in (4, 9)
                then coalesce(AnswerResponse_Value, 'Blank')
                when AnswerResponse_Value is NULL
                then 'Blank'
                when Answer_ResponderId is not NULL or AnswerResponse_Value is not null
                then case when len(AnswerResponse_Value) = 0 then 'Blank' else AnswerResponse_Value end
            end AnswerResponse_Value,
            -- Explanatory column from App
            case
                when Question_HiddenInSurveyForConditional = 1
                then 'Blank because skip logic is applied'
                when Question_Type in (4, 9)
                then Answer_TextArea
                when Answer_ResponderId is not NULL or AnswerResponse_Value is not null
                then Answer_TextArea
                when Answer_ResponderId is NULL
                then 'Blank because Question is not responded to'
            end Answer_TextArea,
            -- Question Status displayed in Drill Thru report
            case
                when Question_HiddenInSurveyForConditional = 1
                then 'Skip Logic Applied'
                when Answer_ResponderId is not NULL or AnswerResponse_Value is not null
                then 'Responded'
                when Answer_ResponderId is NULL
                then 'Not Answered'
            end Question_Status,
            -- Answer Score is a metric that is aggregated, NULL or zero logic follows spec
            case
                when Question_HiddenInSurveyForConditional = 1
                then NULL
                when Question_Type in (4, 9)
                then NULL
                when Answer_ResponderId is not NULL or AnswerResponse_Value is not null
                then Answer_Score
                when Answer_ResponderId is NULL
                then 0
            end Answer_Score,
            -- Risk Status is the raw risk score stored by the app and needs to be transformed to be used as a metric
            case
                when Question_HiddenInSurveyForConditional = 1
                then NULL
                when Question_Type in (4, 9)
                then NULL
                when Answer_ResponderId is not NULL or AnswerResponse_Value is not null
                then Answer_RiskStatus
                when Answer_ResponderId is NULL
                then 0
            end Answer_RiskStatus,
            -- Risk Rating Label is used in group by in count chart, cannot be NULL
            case
                when Question_HiddenInSurveyForConditional = 1
                then 'Skip Logic Applied'
                when Question_Type in (4, 9)
                then 'Not Risk Rated'
                when Answer_ResponderId is not NULL or AnswerResponse_Value is not null
                then Answer_RiskStatusCode
                when Answer_ResponderId is NULL
                then 'Not Answered'
            end Answer_RiskStatusCode,
            -- Risk Status used for metric used for aggregation
            -- when averaged, then need to round and mapped back to the Risk Label between 0 to 5, logic above
            case
                when Question_HiddenInSurveyForConditional = 1
                then NULL
                when Question_Type in (4, 9)
                then NULL
                when Answer_ResponderId is not NULL or AnswerResponse_Value is not null
                then Answer_RiskStatusCalc
                when Answer_ResponderId is NULL
                then 0.00
            end Answer_RiskStatusCalc,
            Question_QuestionGroupId,
            AnswerResponse_PK
        from qa
    )
select  *
from final

{{ config(materialized="view") -}}
with
    q_raw as (  -- dbo.Question with casting
        select
            cast(q.TenantId as int) Question_TenantId,
            cast(q.id as Int) Question_Id,
            -- -
            IdRef Question_IdRef,
            [Name] Question_Name,
            [Description] Question_Description,
            [Order] Question_Order,
            [File] Question_File,
            HiddenInSurveyForConditional Question_HiddenInSurveyForConditional,
            -- -
            cast(q.AssessmentDomainId as int) Question_AssessmentDomainId,
            cast(q. [Type] as int) Question_Type,
            q.ComponentStr Question_ComponentStr,
            cast(q.Weighting as int) Question_Weighting,
            coalesce(LastModificationTime, CreationTime) Question_UpdateTime
        from {{ source("assessment_models", "Question") }} q
    ),
    q as (  -- Tidy question option json based on question type
        select
            q.Question_TenantId,
            q.Question_Id,
            -- -
            Question_IdRef,
            Question_Name,
            Question_Description,
            Question_Order,
            Question_File,
            Question_ComponentStr,
            Question_HiddenInSurveyForConditional,
            -- -
            q.Question_AssessmentDomainId,
            q.Question_Type,
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
            end as [Question_TypeCode],
            json_modify(
                case
                    when Question_Type in (1)
                    then json_query(q.Question_ComponentStr, '$.components.input[0].values')
                    when Question_Type in (2, 5, 6, 10)
                    then json_query(q.Question_ComponentStr, '$.components.radiocustom.values')
                    when Question_Type in (3, 7, 8)
                    then json_query(q.Question_ComponentStr, '$.components.multiselect.values')
                    when Question_Type in (4, 9)
                    then '[{"value":"Free Text"}]'
                end,
                'append $',
                json_query('{ "value": "NULL" }')
            ) Question_OptionJson,
            q.Question_Weighting,
            q.Question_UpdateTime
        from q_raw q
    ),
    qq as (  -- multi row questions converted to data type safely using try_convert
        select
            Question_TenantId,
            Question_Id,
            -- -
            Question_IdRef,
            Question_Name,
            Question_Description,
            Question_Order,
            Question_File,
            Question_ComponentStr,
            Question_HiddenInSurveyForConditional,
            -- -
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Question_Weighting,
            cast(question_kv. [key] + 1 as int) QuestionOption_Key,
            cast(json_value(question_kv. [value], '$.value') as nvarchar(4000)) QuestionOption_Value,
            try_convert(int, json_value(question_kv. [value], '$.rank')) QuestionOption_Rank,
            cast(
                case
                    json_value(question_kv. [value], '$.isTargetResponse') when 'true' then 1 when 'false' then 0 else 0
                end as int
            ) IsTargetResponse,
            try_convert(int, json_value(question_kv. [value], '$.riskStatus')) QuestionOption_RiskStatus,
            case
                when json_value(question_kv. [value], '$.riskStatus') = '0'
                then 'No Risk'
                when json_value(question_kv. [value], '$.riskStatus') = '6'
                then 'Very Low Risk'
                when json_value(question_kv. [value], '$.riskStatus') = '1'
                then 'Low Risk'
                when json_value(question_kv. [value], '$.riskStatus') = '3'
                then 'Medium Risk'
                when json_value(question_kv. [value], '$.riskStatus') = '4'
                then 'High Risk'
                when json_value(question_kv. [value], '$.riskStatus') = '5'
                then 'Very High Risk'
                when json_value(question_kv. [value], '$.riskStatus') is NULL
                then 'No Response'
                else 'Undefined'
            end as QuestionOption_RiskStatusCode,
            case
                json_value(question_kv. [value], '$.riskStatus')
                when '0'
                then 0.0
                when '6'
                then 1.0
                when '1'
                then 2.0
                when '3'
                then 3.0
                when '4'
                then 4.0
                when '5'
                then 5.0
                else NULL
            end QuestionOption_RiskStatusCalc,
            Question_UpdateTime
        from q outer apply openjson(Question_OptionJson, '$') question_kv
    ),
    final as (
        select
            Question_TenantId,
            Question_Id,
            -- -
            Question_IdRef,
            Question_Name,
            Question_Description,
            Question_Order,
            Question_File,
            Question_ComponentStr,
            Question_HiddenInSurveyForConditional,
            -- -
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Question_Weighting,
            sum(cast(Question_Weighting as bigint) * cast(QuestionOption_Rank as bigint)) over (
                partition by Question_Id
            ) Question_MaxPossibleScore,
            QuestionOption_Key,
            QuestionOption_Value,
            QuestionOption_Rank,
            IsTargetResponse QuestionOption_IsTargetResponse,
            QuestionOption_RiskStatus,
            QuestionOption_RiskStatusCode,
            QuestionOption_RiskStatusCalc,
            Question_UpdateTime,
            cast(Question_Id as varchar(10)) + '_' + cast(QuestionOption_Key as varchar(10)) QuestionOption_PK
        from qq
    )
select *
from final

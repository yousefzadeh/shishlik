with
    QUESTION as (
        select *
        from {{ ref("vwQuestionAll") }}
        where Question_Type = 3 and Question_IsDeleted = 0
        union all
        select *
        from {{ ref("vwQuestionAll") }}
        where Question_Type = 7 and Question_IsDeleted = 0
        union all
        select *
        from {{ ref("vwQuestionAll") }}
        where Question_Type = 8 and Question_IsDeleted = 0
    ),
    answer_raw as (
        select
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
            [ComponentStr],
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
            CONCAT(AssessmentResponseId, QuestionId, tenantID) as PK,
            coalesce(LastModificationTime, CreationTime) UpdateTime
        from {{ source("assessment_models", "Answer") }}
        where IsDeleted = 0
    ),
    ANSWER as (
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
            [PK] as [Answer_PK],
            [UpdateTime] as [Answer_UpdateTime]
        from answer_raw
        where QuestionId in (select Question_Id from Question)
    ),
    -- -------- multi-select answer
    q_choose_many as (  -- Tidy question option json based on question type
        select
            [Question_ID],
            [Question_Weighting],
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
            Question_UpdateTime
        from QUESTION q
    ),
    qq_str as (  -- multi row questions
        select
            [Question_ID],
            [Question_Weighting],
            -- Unpack json
            cast(question_kv. [key] + 1 as int) QuestionOption_Key,
            cast(json_value(question_kv. [value], '$.value') as nvarchar(4000)) QuestionOption_Value,
            coalesce(json_value(question_kv. [value], '$.rank'), '0') QuestionOption_Rank,
            cast(
                case
                    json_value(question_kv. [value], '$.isTargetResponse') when 'true' then 1 when 'false' then 0 else 0
                end as int
            ) QuestionOption_IsTargetResponse,
            json_value(question_kv. [value], '$.riskStatus') QuestionOption_RiskStatus,
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
        from q_choose_many outer apply openjson(Question_OptionJson, '$') question_kv
    ),
    qq as (  -- multi row questions
        select
            [Question_ID],
            [Question_Weighting],
            -- Unpack json
            QuestionOption_Key,
            QuestionOption_Value,
            try_convert(int, QuestionOption_Rank) QuestionOption_Rank,
            QuestionOption_IsTargetResponse,
            try_convert(int, QuestionOption_RiskStatus) QuestionOption_RiskStatus,
            QuestionOption_RiskStatusCode,
            QuestionOption_RiskStatusCalc,
            Question_UpdateTime
        from qq_str
    ),
    qq_choose_many as (
        select
            [Question_ID],
            [Question_Weighting],
            QuestionOption_Key,
            QuestionOption_Value,
            QuestionOption_Rank,
            cast(qq.Question_Weighting * qq.QuestionOption_Rank as decimal(7, 2)) QuestionOption_WeightedScore,
            -- ,sum(CAST(Question_Weighting as bigint) * CAST(QuestionOption_Rank as bigint))  over (partition by
            -- Question_Id)  Question_MaxPossibleScore
            QuestionOption_IsTargetResponse,
            QuestionOption_RiskStatus,
            QuestionOption_RiskStatusCode,
            QuestionOption_RiskStatusCalc,
            cast(Question_Id as varchar(10)) + '_' + cast(QuestionOption_Key as varchar(10)) QuestionOption_PK,
            Question_UpdateTime
        from qq
    ),
    -- -------
    a_choose_many as (
        -- join question to answer 
        -- tidy answer json based on question type.
        -- prepare answer json as an array for cross apply openjson()
        select
            a.*, '{ "answer" : ' + json_query(a.Answer_ComponentStr, '$.MultiSelectValues') + ' }' Answer_ResponseJson  -- json array for cross apply openjson()
        from q_choose_many q
        join ANSWER a on q.Question_Id = a.Answer_QuestionId
    ),
    aa_choose_many as (  -- multi row answers
        select
            [Answer_ID],
            [Answer_QuestionId],
            cast(answer_kv. [key] + 1 as int) AnswerResponse_Key,
            cast(answer_kv. [value] as nvarchar(200)) AnswerResponse_Value,
            cast(qa.Answer_Id as varchar(10)) + '_' + answer_kv. [key] AnswerResponse_PK,
            Answer_UpdateTime
        from a_choose_many qa outer apply openjson(qa.Answer_ResponseJson, '$.answer') answer_kv
    ),
    null_answer as (
        -- Add row for Null Answers
        select
            0 [Answer_ID],
            0 [Answer_QuestionId],
            0 AnswerResponse_Key,
            'Undefined' AnswerResponse_Value,
            '0_0' AnswerResponse_PK,
            '2000-01-01 00:00:01.000' Answer_UpdateTime
        from {{ source("assessment_models", "AbpTenants") }}
    ),
    aa_all as (
        select *
        from aa_choose_many
        union all
        select *
        from null_answer
    ),
    -- -------
    final as (
        select
            -- qq join aa
            [Answer_ID],
            Question_ID,
            AnswerResponse_Key,
            QuestionOption_WeightedScore as AnswerResponse_WeightedScore,
            sum(QuestionOption_WeightedScore) over (partition by Question_Id) as AnswerResponse_MaxPossibleScore,
            aa.AnswerResponse_Value,
            greatest(aa.Answer_UpdateTime, qq.Question_UpdateTime) AnswerResponse_UpdateTime,
            AnswerResponse_PK
        from qq_choose_many qq
        left join
            aa_all as aa on qq.Question_Id = aa.Answer_QuestionId and qq.QuestionOption_Value = aa.AnswerResponse_Value
    )
select *
from final

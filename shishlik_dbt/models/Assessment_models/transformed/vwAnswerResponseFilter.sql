with
    a as (  -- dbo.Answer with casting and add calculated columns
        select
            cast(a.TenantId as Int) Answer_TenantId,
            cast(a.Id as int) Answer_Id,
            cast(a.QuestionId as int) Answer_QuestionId,
            case when a.ComponentStr = '' then NULL else  a.ComponentStr 
                end as Answer_ComponentStr,
	    	cast(coalesce(a.LastModificationTime, a.CreationTime) as datetime2) Answer_LastModificationTime
        from {{ source("assessment_models", "Answer") }} a
    ),
    q as (  -- dbo.Question with casting
        select
            cast(q.TenantId as int) Question_TenantId,
            cast(q.id as Int) Question_Id,
            cast(q. [Type] as int) Question_Type,
    		cast(coalesce(q.LastModificationTime, q.CreationTime) as datetime2) as Question_LastModificationTime
        from {{ source("assessment_models", "Question") }} q
    )
, qa_single as (
    select
        Answer_TenantId,
        Answer_QuestionId,
        Answer_Id,
        0 AnswerResponse_Key,
        case
            when Question_Type in (1)
            then coalesce(json_value(a.Answer_ComponentStr, '$.Radio'), 'Blank')
            when Question_Type in (2, 5, 6, 10)
            then coalesce(json_value(a.Answer_ComponentStr, '$.RadioCustom'), 'Blank')
            when Question_Type in (4, 9)
            then 'Free Text'  -- Match the value in Question
        end AnswerResponse_Value,
		case when a.Answer_LastModificationTime >= q.Question_LastModificationTime then a.Answer_LastModificationTime 
            else q.Question_LastModificationTime  end as ARF_Updatetime
    from q
    join a on q.Question_Id = a.Answer_QuestionId
    where Question_Type not in (3, 7, 8)
),
qa_multi_json as (
    -- join question to answer 
    -- tidy answer json based on question type.
    -- prepare answer json as an array for cross apply openjson()
    select
        Answer_TenantId,
        Answer_QuestionId,
        Answer_Id,
        '{ "answer" : ' + json_query(a.Answer_ComponentStr, '$.MultiSelectValues') + ' }' Answer_ResponseJson,
		case when a.Answer_LastModificationTime >= q.Question_LastModificationTime then a.Answer_LastModificationTime 
            else q.Question_LastModificationTime  end as ARF_Updatetime
    from q
    join a on q.Question_Id = a.Answer_QuestionId
    where Question_Type in (3, 7, 8)
),
qa_multi as (
    select 
        Answer_TenantId,
        Answer_QuestionId,
        Answer_Id,
        cast(answer_kv. [key] + 1 as int) AnswerResponse_Key,
        cast(answer_kv. [value] as nvarchar(500)) AnswerResponse_Value,
		ARF_Updatetime
    from qa_multi_json qajs 
	outer apply openjson(qajs.Answer_ResponseJson, '$.answer') answer_kv
),
qa as (
        select Answer_TenantId,
            Answer_QuestionId,
            Answer_Id,
            AnswerResponse_Key,
            AnswerResponse_Value,
            ARF_Updatetime
        from qa_single
        union all
        select Answer_TenantId,
            Answer_QuestionId,
            Answer_Id,
            AnswerResponse_Key,
            AnswerResponse_Value,
            ARF_Updatetime
        from qa_multi
),
null_answer as (
    -- Add row for Null Answers
    select DISTINCT
        0 Answer_TenantId, 0 Answer_QuestionId, 0 Answer_Id, 0 AnswerResponse_Key, 'Undefined' AnswerResponse_Value, '2000-01-01 00:00:01.000' as NULL_dates
),
final as (
    select  Answer_TenantId,
            Answer_QuestionId,
            Answer_Id,
            coalesce(AnswerResponse_Key,0) as AnswerResponse_Key,
            AnswerResponse_Value,
            ARF_Updatetime
    from qa
    union all
    select Answer_TenantId,
            Answer_QuestionId,
            Answer_Id,
            AnswerResponse_Key,
            AnswerResponse_Value,
            NULL_dates
    from null_answer
)
select  Answer_TenantId,
            Answer_QuestionId,
            Answer_Id,
            AnswerResponse_Key,
            AnswerResponse_Value,
            ARF_Updatetime,
        	rank() OVER (ORDER BY Answer_Id,AnswerResponse_Key) as AnswerResponseFilter_pk
from final

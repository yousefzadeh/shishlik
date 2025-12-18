{# 
DOC START
  - name: vwAnswerResponseChooseMany
    description: |
      In the App, the Answer Table is not populating the fields correctly for this question "Choose Many" type and answer "Multi-Select" type. 
      The purpose of this view is to backfill the incorrect values in the Answer table.
      Grain: One row of each Answer and Response for "Multiselect" type for "Choose Many" questions.
      In the case of Multi-choice question, the weighted score of each possible option is in the json of ComponentStr of Question table under "rank" key.
      In the case of Multi-Select type answer, the selected options are in the json of ComponentStr of Answer table under "value" key.
      Columns that are incorrect and re-calculated by this view:
      - AnswerResponse_Key: Sequence number for each Multi-Select answer response
      - Answer_AnswerText: Selected option or freetext for the answer
      - Answer_MaxPossibleScore: Max possible score for this question across all possible options 
      - Answer_Score: Weighted score for the selected option
      - Answer_ResponseCount: 1 for each Multi-Select answer response
    columns:
      - name: Answer_ID
        description: FK to Answer table
      - name: Question_ID
        description: FK to Question table
      - name: AnswerResponse_Key
        description: Seq number of the Response row for the answer
      - name: AnswerResponse_WeightedScore
        description: For each Multi-select response, WeightedScore = Question_Weighting(Question_Id row) * QuestionOption_Rank(Question_Id/Json-Option row) 
      - name: AnswerResponse_MaxPossibleScore
        description: For the Question_Id of the response, max possible score across all possible options for the question
      - name: AnswerResponse_Value
        description: Response Value selected or entered
      - name: AnswerResponse_PK
        description: Constructed Primary Key for the response row
DOC END
#}

with
    Question as (
        {# All questions not deleted of the "Choose Many" type -#}
        select 
        [Type] Question_Type,
        IsDeleted Question_IsDeleted,
        Id Question_Id,
        Weighting Question_Weighting,
        ComponentStr Question_ComponentStr,
        IsMultiSelectType Question_IsMultiSelectType,
        cast(coalesce(LastModificationTime, CreationTime) as datetime2) as Question_LastModificationTime --date column addition for synapse incremental load
        from {{ source("assessment_models","Question") }} q
        where IsMultiSelectType = 1 and IsDeleted = 0
    ),
    Answer as (
        {# All Answers not deleted -#}
        select
            Id Answer_Id,
            QuestionId Answer_QuestionId,
            ComponentStr Answer_ComponentStr,
             cast(coalesce(LastModificationTime, CreationTime) as datetime2) Answer_LastModificationTime --date column addition for synapse incremental load
        from {{ source("assessment_models", "Answer") }}
        where IsDeleted = 0
    ),
    -- -------- multi-select answer
    q_choose_many as (  -- Tidy question option json based on question type
        select
            Question_ID,
            Question_Weighting,
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
		    Question_LastModificationTime
        from QUESTION q
    ),
    qq_str as (  -- multi row questions
        select
            Question_ID,
            Question_Weighting,
            -- Unpack json
            cast(question_kv. [key] + 1 as int) QuestionOption_Key,
            cast(json_value(question_kv. [value], '$.value') as nvarchar(4000)) QuestionOption_Value,
            coalesce(json_value(question_kv. [value], '$.rank'), '0') QuestionOption_Rank,
		    Question_LastModificationTime
        from q_choose_many outer apply openjson(Question_OptionJson, '$') question_kv
    ),
    qq as (  -- multi row questions
        select
            Question_ID,
            Question_Weighting,
            -- Unpack json
            QuestionOption_Key,
            QuestionOption_Value,
            try_convert(int, QuestionOption_Rank) QuestionOption_Rank,
	    	Question_LastModificationTime
        from qq_str
    ),    
    qq_choose_many as (
        select
            Question_ID,
            Question_Weighting,
            QuestionOption_Key,
            QuestionOption_Value,
            QuestionOption_Rank,
            cast(qq.Question_Weighting * qq.QuestionOption_Rank as decimal(38, 2)) QuestionOption_WeightedScore,
		    Question_LastModificationTime
            {# sum(CAST(Question_Weighting as bigint) * CAST(QuestionOption_Rank as bigint))  over (partition by Question_Id)  Question_MaxPossibleScore, -#}
        from qq
    ),
    -- -------
    a_choose_many as (
        -- join question to answer 
        -- tidy answer json based on question type.
        -- prepare answer json as an array for cross apply openjson()
        select
            a.Answer_ID,
            a.Answer_QuestionId,
            '{ "answer" : ' + json_query(a.Answer_ComponentStr, '$.MultiSelectValues') + ' }' Answer_ResponseJson,  -- json array for cross apply openjson()
		    greatest(q.Question_LastModificationTime,a.Answer_LastModificationTime) as UpdateTime
        from q_choose_many q
        join ANSWER a on q.Question_Id = a.Answer_QuestionId
        where ISJSON (a.Answer_ComponentStr) > 0
    ),
    aa_choose_many as (  -- multi row answers
        select 
            qa.Answer_ID,
            qa.Answer_QuestionId,
            cast(answer_kv. [key] + 1 as int) AnswerResponse_Key,
            answer_kv. [value] AnswerResponse_Value,
            cast(qa.Answer_Id as varchar(10)) + '_' + answer_kv. [key] AnswerResponse_PK, 
		    qa.UpdateTime
        from a_choose_many qa outer apply openjson(qa.Answer_ResponseJson, '$.answer') answer_kv
    ),
    null_answer as (
        -- Add row for Null Answers
        select 
            0 Answer_ID,
            0 Answer_QuestionId,
            0 AnswerResponse_Key,
            'Undefined' AnswerResponse_Value,
            '0_0' AnswerResponse_PK,
            '2000-01-01 00:00:00' as UpdateTime
    ),
    aa_all as (
        select Answer_ID,
            Answer_QuestionId,
            AnswerResponse_Key,
            AnswerResponse_Value,
            AnswerResponse_PK,
            UpdateTime
        from aa_choose_many
        union 
        select Answer_ID,
        Answer_QuestionId,
        AnswerResponse_Key,
        AnswerResponse_Value,
        AnswerResponse_PK,
        UpdateTime
        from null_answer
    ),
    -- -------
    final as (
        select DISTINCT
        -- qq join aa
            coalesce(aa.Answer_ID,0) as Answer_ID,--Fixed NUlls to supposrt constraint integirty
            qq.Question_ID,
             coalesce(aa.AnswerResponse_Key,0) as AnswerResponse_Key,--Fixed NUlls to supposrt constraint integirty
            case
                when aa.AnswerResponse_Value is null then 0.0 else qq.QuestionOption_WeightedScore
            end as AnswerResponse_WeightedScore,
            sum(qq.QuestionOption_WeightedScore) over (partition by qq.Question_Id) as AnswerResponse_MaxPossibleScore,
            aa.AnswerResponse_Value,
            aa.AnswerResponse_PK,
             greatest(qq.Question_LastModificationTime,coalesce(aa.UpdateTime, '2000-01-01 00:00:01.000')) as ARCM_UpdateTime --Picking the most recent date columns wrt Question and answer updates
        from qq_choose_many qq
        left join
            aa_all as aa
            on qq.Question_Id = aa.Answer_QuestionId
            and qq.QuestionOption_Value = coalesce(aa.AnswerResponse_Value, qq.QuestionOption_Value)
    )
select 
    [Answer_ID],
    [Question_ID] ,
    [AnswerResponse_Key],
    [AnswerResponse_WeightedScore],
    [AnswerResponse_MaxPossibleScore],
    [AnswerResponse_Value],
    [AnswerResponse_PK],
    [ARCM_UpdateTime]
from final
--where Question_Id in( 3704,14583,14587,15439)
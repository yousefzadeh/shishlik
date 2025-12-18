   with
    qqaa_choose_many as ( --12507
	select  [Answer_ID] 
			, [Question_Id]
			, [AnswerResponse_WeightedScore]
			, [AnswerResponse_MaxPossibleScore]
			, [AnswerResponse_Value]
			, [ARCM_UpdateTime]--date column addition for synapse incremental load
	from {{ ref("vwAnswerResponseChooseMany") }}
    ),
    qa_choose_many as (
        select 
            [Answer_ID],
            [Question_Id],
            sum(AnswerResponse_WeightedScore) Answer_Score,
            max(AnswerResponse_MaxPossibleScore) Answer_MaxPossibleScore,
            count(*) Answer_ResponseCount,
            string_agg(AnswerResponse_Value, ',') Answer_ResponseValue,
			max(ARCM_UpdateTime) as AnswerChooseMany_UpdateTime -- MAX of date to remove duplcation caused by string agg function
        from qqaa_choose_many
        group by Question_Id, Answer_Id
    )
select [Answer_ID],
        [Question_Id],
        [Answer_Score],
        [Answer_MaxPossibleScore],
        [Answer_ResponseCount],
        [Answer_ResponseValue],
        [AnswerChooseMany_UpdateTime]--date column addition for synapse incremental load
from qa_choose_many

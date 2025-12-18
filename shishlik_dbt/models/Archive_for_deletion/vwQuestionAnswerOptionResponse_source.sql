{{- config(materialized="view") -}}
{#- 
  Grain: One Question, One Answer, One Answer Response

  max updatetime of Question and Answer may be different
  always use the Question_UpdateTime and ignore the Answer_UpdateTime
-#}
with
    qq_all as (select * from {{ ref("vwQuestionOption_lambda") }}),
    aa_all as (select * from {{ ref("vwAnswerResponse_lambda") }}),
    qqaa as (
        select
            qq.Question_TenantId,
            qq.Question_Id,
            qq.Question_AssessmentDomainId,
            qq.Question_Type,
            qq.Question_TypeCode,
            qq.Question_Weighting,
            qq.Question_MaxPossibleScore,
            qq.QuestionOption_Key,
            qq.QuestionOption_Value,
            qq.QuestionOption_Rank,
            qq.QuestionOption_IsTargetResponse,
            qq.QuestionOption_RiskStatus,
            qq.QuestionOption_RiskStatusCode,
            qq.QuestionOption_RiskStatusCalc,
            aa.Answer_Id,
            aa.Answer_Version,
            aa.Answer_IsCurrent,
            aa.Answer_Score,
            case
                when aa.AnswerResponse_Key is not null
                then cast(qq.Question_Weighting * qq.QuestionOption_Rank as decimal(7, 2))
            end as AnswerResponse_WeightedScore,
            case
                when aa.AnswerResponse_Key is not null then cast(qq.QuestionOption_RiskStatus as int)
            end as AnswerResponse_RiskStatus,
            case
                when aa.AnswerResponse_Key is not null then cast(qq.QuestionOption_RiskStatusCode as varchar(20))
            end as AnswerResponse_RiskStatusCode,
            case
                when aa.AnswerResponse_Key is not null then cast(qq.QuestionOption_RiskStatusCalc as decimal(7, 2))
            end as AnswerResponse_RiskStatusCalc,
            case
                when aa.AnswerResponse_Key is not null then cast(qq.Question_MaxPossibleScore as decimal(7, 2))
            end as AnswerResponse_MaxPossibleScore,
            aa.AnswerResponse_Key,
            case
                when aa.AnswerResponse_Value = 'NULL' then 'Attempted but No Response' else aa.AnswerResponse_Value
            end AnswerResponse_Value,
            aa.Answer_TextArea
        {# ,
  greatest(qq.Question_UpdateTime, coalesce(aa.Answer_UpdateTime,qq.Question_UpdateTime)) QuestionAnswer_UpdateTime #}
        from qq_all qq
        left join
            aa_all as aa on qq.Question_Id = aa.Answer_QuestionId and qq.QuestionOption_Value = aa.AnswerResponse_Value
    ),
    final as (
        select
            Question_TenantId,
            Question_Id,
            Question_AssessmentDomainId,
            Question_Type,
            Question_TypeCode,
            Question_Weighting,
            Question_MaxPossibleScore,  -- calculated 
            QuestionOption_Key,
            QuestionOption_Value,
            QuestionOption_Rank,
            QuestionOption_IsTargetResponse,
            QuestionOption_RiskStatus,
            QuestionOption_RiskStatusCode,
            QuestionOption_RiskStatusCalc,  -- calculated
            Answer_Id,
            Answer_Version,
            Answer_IsCurrent,
            Answer_Score,
            AnswerResponse_WeightedScore,  -- calculated
            AnswerResponse_RiskStatus,
            AnswerResponse_RiskStatusCode,
            AnswerResponse_RiskStatusCalc,
            AnswerResponse_MaxPossibleScore,  -- copy from Question_MaxPossibleScore
            AnswerResponse_Key,  -- same grain as QuestionOption_Key
            AnswerResponse_Value,
            Answer_TextArea,
            {# QuestionAnswer_UpdateTime, #}
            'Q'
            + cast(Question_Id as varchar(10))
            + '_'
            + cast(QuestionOption_Key as varchar(10))
            + '_A'
            + coalesce(cast(Answer_Id as varchar(10)), '.')
            + '_'
            + coalesce(cast(AnswerResponse_Key as varchar(10)), '.') as QuestionAnswerOptionResponse_PK
        from qqaa
        where Answer_Id is not null  -- Ignore QuestionOptions that are not Selected as a Response
    )
select *
from final

{{- config(materialized="view") -}}
with
    ass_temp as (select * from {{ ref("vwAssessmentAuthority_source") }}),
    answer_response as (select * from {{ ref("vwAnswerResponse_lambda") }}),
    domain as (select * from {{ ref("vwAssessmentDomain") }}),
    final as (
        select
            ass_temp.*,
            domain.AssessmentDomain_Name,
            answer_response.*,
            {# greatest(
        ass_temp.AssessmentTemplate_UpdateTime,
        domain.AssessmentDomain_UpdateTime,
        answer_response.Answer_UpdateTime
    ) AssessmentQuestionAnswer_UpdateTime, -- uncomment when deploying tables and after adding calculated columns UpdateTime in the 3 tables#}
            {{
                safe_concat(
                    ["ass_temp.Assessment_Id", "answer_response.Answer_Id", "answer_response.AnswerResponse_Key"]
                )
            }} AuthorityAssessmentQuestionAnswer_PK
        from ass_temp
        join domain on ass_temp.Assessment_Id = domain.AssessmentDomain_AssessmentId
        join answer_response on domain.AssessmentDomain_Id = answer_response.Question_AssessmentDomainId
    )
select *
from final

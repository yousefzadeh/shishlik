{{ config(materialized="view") }}

with
    tpl as (
        select *
        from {{ ref("vwAssessment") }} a
        where
            a.Assessment_IsTemplate = 1
            and a.Assessment_IsArchived = 0
            and a.Assessment_IsDeprecatedAssessmentVersion = 0
            and a.Assessment_Status = 3
    ),
    ass as (
        select *
        from {{ ref("vwAssessment") }} a
        where
            a.Assessment_WorkFlow = 'Question'  -- QBA only
            and a.Assessment_IsTemplate = 0  -- assessments only
            and a.Assessment_IsArchived = 0
            and a.Assessment_IsDeprecatedAssessmentVersion = 0
            and a.Assessment_Status in (4, 5, 6)  -- completed
    ),
    score as (
        select
            ass.Assessment_TenantId,
            ass.Assessment_Id,
            ass.Assessment_Name,
            ass.Assessment_QuestionType,
            ass.Assessment_StatusCode,
            ass.Assessment_QuestionTypeCode,
            qa.Question_TenantId,
            qa.Question_ID,
            qa.Question_TypeCode,
            qa.Question_Weighting,
            qa.completed_flag,
            qa.Answer_Score,
            qa.Question_Weighting * qa.Answer_Score as WeightedScore,
            qa.Question_RiskStatus,
            qa.Answer_RiskStatus,
            qa.Answer_Compliance
        from ass
        join {{ ref("vwAssessmentDomain") }} ad on ad.AssessmentDomain_AssessmentId = ass.Assessment_Id
        join {{ ref("vwQuestionAnswer") }} qa on qa.Question_AssessmentDomainId = ad.AssessmentDomain_ID
        where ass.Assessment_QuestionType = 1 and qa.Question_Type not in (4, 9)
    ),
    final as (
        select
            Assessment_TenantId,
            Assessment_Id,
            Assessment_Name,
            Assessment_QuestionTypeCode,
            Question_TypeCode,
            Question_Weighting,
            Answer_Score,
            completed_flag,
            WeightedScore score
        from score
        where score.Assessment_QuestionType = 1  -- Weighted score

        union all

        select
            Assessment_TenantId,
            Assessment_Id,
            Assessment_Name,
            Assessment_QuestionTypeCode,
            Question_TypeCode,
            Question_Weighting,
            Answer_Score,
            completed_flag,
            Answer_RiskStatus score
        from score
        where score.Assessment_QuestionType = 2  -- Risk rated
    )

select *
from final
where
    1 = 1
    -- and Question_Type in (3,7,8) -- choose many - NULL score
    -- and Question_Type in (2,5,6,10) -- choose one
    -- and Question_Type in (1) -- Yes No
    -- and Question_Type in (4,9) -- Free text
    

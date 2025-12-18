with
    detail as (
        select
            a.Assessment_ID,
            a.Assessment_Name,
            ad.AssessmentDomain_Order,
            ad.AssessmentDomain_ID,
            ad.AssessmentDomain_Name,
            q.Question_Order,
            q.Question_Name,
            q.Question_ID,
            q.Question_TenantId,
            ans.Answer_RadioCustom,
            ans.Answer_TextArea,
            ans.Answer_Score,
            ans.Answer_MaxPossibleScore,
            ans.Answer_RiskStatus,
            ans.Answer_RiskStatusCalc,
            ans.Answer_RiskStatusCode,
            ans.Answer_AssessmentResponseId,
            ans.Answer_ResponderId
        from {{ ref("vwAssessment") }} a
        inner join {{ ref("vwAssessmentDomain") }} ad on (a.Assessment_ID = ad.AssessmentDomain_AssessmentId)
        inner join {{ ref("vwQuestion") }} q on (ad.AssessmentDomain_ID = q.Question_AssessmentDomainId)
        left join {{ ref("vwAnswer") }} ans on (ans.Answer_QuestionId = q.Question_Id)
        inner join
            {{ ref("vwAssessment") }} a2
            on (a2.Assessment_ID = a.Assessment_CreatedFromTemplateId)
            and (a2.Assessment_IsTemplate = 1 and a2.Assessment_WorkFlowId = 0)
    ),
    min_max_avg as (
        select
            Assessment_ID,
            Assessment_Name,
            AssessmentDomain_ID,
            AssessmentDomain_Name,
            Question_Id,
            Question_TenantId,
            Answer_RadioCustom,
            Answer_Score,
            Answer_MaxPossibleScore,
            Answer_RiskStatus,
            Answer_RiskStatusCalc,
            Answer_RiskStatusCode,
            Answer_ResponderId,
            Answer_AssessmentResponseId,
            count(*) Count_Score,
            min(Answer_Score) Min_Score,
            max(Answer_Score) Max_Score,
            avg(Answer_Score) Avg_Score
        from detail
        where Answer_Score is not null
        group by
            Assessment_ID,
            Assessment_Name,
            AssessmentDomain_ID,
            AssessmentDomain_Name,
            Question_Id,
            Question_TenantId,
            Answer_RadioCustom,
            Answer_Score,
            Answer_MaxPossibleScore,
            Answer_RiskStatus,
            Answer_RiskStatusCalc,
            Answer_RiskStatusCode,
            Answer_ResponderId,
            Answer_AssessmentResponseId
    ),
    uni as (
        select
            AssessmentDomain_ID,
            AssessmentDomain_Name,
            Question_Id,
            Question_TenantId,
            Answer_ResponderId,
            Answer_AssessmentResponseId,
            Answer_RadioCustom,
            Answer_Score,
            Answer_MaxPossibleScore,
            Answer_RiskStatus,
            Answer_RiskStatusCalc,
            Answer_RiskStatusCode,
            Count_Score,
            Min_Score,
            null Max_Score,
            null Avg_Score,
            'Min' rollup
        from min_max_avg a

        -- where Min_Score is not null and a.AssessmentDomain_Name = '5Finance & Legal'
        union all

        select distinct
            AssessmentDomain_ID,
            AssessmentDomain_Name,
            Question_Id,
            Question_TenantId,
            Answer_ResponderId,
            Answer_AssessmentResponseId,
            Answer_RadioCustom,
            Answer_Score,
            Answer_MaxPossibleScore,
            Answer_RiskStatus,
            Answer_RiskStatusCalc,
            Answer_RiskStatusCode,
            a.Count_Score,
            null Min_Score,
            a.Max_Score,
            null Avg_Score,
            'Max' rollup
        from min_max_avg a

        -- where Min_Score is not null and a.AssessmentDomain_Name = '5Finance & Legal'
        union all

        select distinct
            AssessmentDomain_ID,
            AssessmentDomain_Name,
            Question_Id,
            Question_TenantId,
            Answer_ResponderId,
            Answer_AssessmentResponseId,
            Answer_RadioCustom,
            Answer_Score,
            Answer_MaxPossibleScore,
            Answer_RiskStatus,
            Answer_RiskStatusCalc,
            Answer_RiskStatusCode,
            a.Count_Score,
            null Min_Score,
            null Max_Score,
            a.Avg_Score,
            'Mean' rollup
        from min_max_avg a

    -- where Min_Score is not null and a.AssessmentDomain_Name = '5Finance & Legal'
    )

select *
from uni

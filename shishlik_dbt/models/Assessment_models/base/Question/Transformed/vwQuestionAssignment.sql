{{ config(materialized="view") }}

with
    question_assignment_by_user as (
        -- Get all question assignments for users assigned to specific questions
        select DISTINCT--Distinct to remove overall duplicates 
            a.Assessment_ID,
            a.Assessment_TenantId TenantId,
            qa.Question_ID,
            qvu.QuestionVendorUser_UserId as AssigneeUserID,
            qa.Answer_ID,
            qa.Answer_ResponderId,
	        qa.QA_UpdateTime--date column addition FROM vwQuestionAnswer for synapse incremental load
        from {{ ref("vwAssessment") }} a
        inner join {{ ref("vwAssessmentDomain") }} ad on a.Assessment_ID = ad.AssessmentDomain_AssessmentId
        inner join {{ ref("vwQuestionAnswer") }} qa on ad.AssessmentDomain_ID = qa.Question_AssessmentDomainId
        left join {{ ref("vwQuestionVendorUser") }} qvu on qa.Question_ID = qvu.QuestionVendorUser_QuestionId
    -- INNER JOIN {{ref('vwAbpUser')}} u
    -- ON qvu.QuestionVendorUser_ID = u.AbpUsers_Id 
    -- where a.Assessment_ID = 1313
    ),
    all_question_assignment_by_user as (
        -- Get all question assignments for users assigned to all questions (not specific questions)
        select DISTINCT--Distinct to remove overall duplicates
            atvu.AssessmentId Assessment_ID,
            atvu.TenantId,
            qa1.Question_ID,
            atvu.UserId as AssigneeUserId,
            qa1.Answer_ID,
            qa1.Answer_ResponderId,
	        qa1.QA_UpdateTime--date column addition for synapse incremental load
        from {{ source("assessment_models", "QuestionVendorUser") }} atvu
        inner join {{ ref("vwAssessmentDomain") }} ad1 on atvu.AssessmentId = ad1.AssessmentDomain_AssessmentId
        inner join
            {{ ref("vwQuestionAnswer") }} qa1
            on ad1.AssessmentDomain_ID = qa1.Question_AssessmentDomainId
            -- where AssessmentId = 1313
            and not exists (
                select a.assessment_ID, qvu.QuestionVendorUser_UserId as AssigneeUserID
                from {{ ref("vwAssessment") }} a
                inner join {{ ref("vwAssessmentDomain") }} ad on a.Assessment_ID = ad.AssessmentDomain_AssessmentId
                inner join {{ ref("vwQuestionAnswer") }} qa on ad.AssessmentDomain_ID = qa.Question_AssessmentDomainId
                inner join {{ ref("vwQuestionVendorUser") }} qvu on qa.Question_ID = qvu.QuestionVendorUser_QuestionId
                where atvu.AssessmentId = a.assessment_Id and atvu.UserId = qvu.QuestionVendorUser_UserId
            )
    )
, final as (
    select DISTINCT--Distinct to remove overall duplicates
        [Assessment_ID]
        ,[TenantId]
        ,[Question_ID]
        ,[AssigneeUserId]
        ,[Answer_ID]
        ,[Answer_ResponderId]
        ,[QA_UpdateTime]--date column addition for synapse incremental load
        , rank() OVER (ORDER BY Assessment_ID, TenantId,Question_ID, coalesce(AssigneeUserId,0), coalesce(Answer_ID,0), coalesce(Answer_ResponderId,0) ) AS QuestionAssignment_pk 
        -- added a QuestionAssignment_pk for constraint integirty 
        from (
                select *
                from all_question_assignment_by_user
                union all
                select *
                from question_assignment_by_user
        )a
)
SELECT cast(QuestionAssignment_pk as int) as QuestionAssignment_pk
        ,[Assessment_ID]
      ,[TenantId]
      ,[Question_ID]
      ,[AssigneeUserId]
      ,[Answer_ID]
      ,[Answer_ResponderId]
      ,[QA_UpdateTime]--date column addition for synapse incremental load
from final

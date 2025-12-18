-- - find the list of authority and templates that have questions and answers of diferent types
{{ config(materialized="view") }}

with
    ass_ans as (
        select
            a.Assessment_ID,
            a.Assessment_IsTemplate,
            a.Assessment_QuestionTypeCode,
            a.Assessment_CreatedFromTemplateId,
            ad.AssessmentDomain_ID,
            ad.AssessmentDomain_Name,
            q.Question_ID,
            q.Question_TypeCode,
            q.Question_Weighting,
            ans.Answer_ID,
            ans.Answer_RiskStatus,
            ans.Answer_RiskStatusCode,
            ans.Answer_Score
        from {{ ref("vwAssessment") }} a
        join {{ ref("vwAssessmentDomain") }} ad on ad.AssessmentDomain_AssessmentId = a.Assessment_ID
        join {{ ref("vwQuestion") }} q on q.Question_AssessmentDomainId = ad.AssessmentDomain_ID
        left join {{ ref("vwAnswer") }} ans on q.Question_ID = ans.Answer_QuestionId
        where Assessment_IsTemplate = 0
    ),
    detail as (
        select
            case
                when tpl.Assessment_AuthorityId is null and ap.AuthorityPolicy_AuthorityId is null
                then 'No authority'
                when tpl.Assessment_AuthorityId is null
                then 'Related authority'
                else 'Direct authority'
            end Relation,
            coalesce(tpl.Assessment_AuthorityId, ap.AuthorityPolicy_AuthorityId) Authority_Id,
            tpl.Assessment_ID TemplateId,
            tpl.Assessment_TemplateVersion TemplateVersion,
            tpl.Assessment_TenantId Template_TenantId,
            {# tpl.Assessment_QuestionTypeCode Template_AssessmentStyle,  #}
            ass_ans.Assessment_Id,
            ass_ans.Assessment_QuestionTypeCode AssessmentStyle,
            Question_TypeCode Question_Type,
            Question_Id,
            Answer_Id
        from {{ ref("vwAssessment") }} tpl
        join ass_ans on tpl.Assessment_IsTemplate = 1 and tpl.Assessment_ID = ass_ans.Assessment_CreatedFromTemplateId
        left join {{ ref("vwAuthorityPolicy") }} ap on ap.AuthorityPolicy_PolicyId = tpl.Assessment_PolicyId
        left join {{ ref("vwAuthority") }} a on ap.AuthorityPolicy_AuthorityId = a.Authority_Id
    )

select detail.*, a.Authority_Name, t.Assessment_Name
from detail
left join {{ ref("vwAuthority") }} a on detail.Authority_Id = a.Authority_Id
left join {{ ref("vwAssessment") }} t on detail.TemplateId = t.Assessment_Id

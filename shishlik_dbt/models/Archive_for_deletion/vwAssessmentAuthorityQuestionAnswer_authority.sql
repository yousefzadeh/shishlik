-- Case Assessment linked to Authority 
with
    final as (
        select aa.*, ad.Name DomainName, qa.*, pq.AuthorityProvisionId ProvisionQuestion_AuthorityProvisionId
        from {{ ref("vwAssessmentAuthority_authority") }} aa
        join {{ source("assessment_models", "AssessmentDomain") }} ad on aa.Assessment_Id = ad.AssessmentId
        join {{ ref("vwQuestionAnswerResponse_V2") }} qa on ad.Id = qa.Question_AssessmentDomainId
        join
            {{ source("assessment_models", "ProvisionQuestion") }} pq
            on qa.Question_Id = pq.QuestionId
            and aa.Assessment_TenantId = pq.TenantId
    )
select *
from final

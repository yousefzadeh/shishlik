-- Case Assessment linked to Controlset To Authority 
with
    final as (
        select aa.*, ad.Name DomainName, qa.*, 0 Unlinked_AuthorityProvisionId
        from {{ ref("vwAssessmentAuthority_unlinked") }} aa
        join {{ source("assessment_models", "AssessmentDomain") }} ad on aa.Assessment_Id = ad.AssessmentId
        join {{ ref("vwQuestionAnswerResponse_V2") }} qa on ad.Id = qa.Question_AssessmentDomainId
    )
select *
from final

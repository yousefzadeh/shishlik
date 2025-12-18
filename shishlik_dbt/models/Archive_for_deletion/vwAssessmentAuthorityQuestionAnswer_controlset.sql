-- Case Assessment linked to Controlset To Authority 
with
    final as (
        select aa.*, ad.Name DomainName, qa.*, pc.AuthorityReferenceId ProvisionControl_AuthorityProvisionId
        from {{ ref("vwAssessmentAuthority_controlset") }} aa
        join {{ source("assessment_models", "AssessmentDomain") }} ad on aa.Assessment_Id = ad.AssessmentId
        join {{ ref("vwQuestionAnswerResponse_V2") }} qa on ad.Id = qa.Question_AssessmentDomainId
        join
            {{ source("assessment_models", "ControlQuestion") }} cq
            on qa.Question_Id = cq.QuestionId
            and aa.Assessment_TenantId = cq.TenantId
        join
            {{ source("assessment_models", "ProvisionControl") }} pc
            on cq.ControlsId = pc.ControlsId
            and cq.TenantId = aa.Assessment_TenantId
    )
select *
from final

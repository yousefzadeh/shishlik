-- Grain of Data for aggregate scoring:
-- Tenant ID
-- Assessment Template ID
-- Assessment ID
-- Provision ID
-- Question ID
-- Answer ID
select
    tpl.Template_TenantId,
    tpl.Template_AuthorityId,  -- Authority ID
    tpl.Template_PolicyId,  -- Policy ID
    case
        when tpl.Template_AuthorityId is not null then 'Thru Authority' else 'Thru Control Set'
    end as Template_Relation,
    tpl.Template_Id,
    tpl.Template_Name,
    tpl.Template_TemplateVersion Template_Version,
    tpl.Template_QuestionTypeCode Template_AssessmentStyle,
    ass.Assessment_TenantId,
    ass.Assessment_CreatedFromTemplateId,
    ass.Assessment_Id,
    ass.Assessment_Name,
    ass.Assessment_QuestionType,
    ass.Assessment_StatusCode,
    ass.Assessment_QuestionTypeCode,
    ass.Assessment_EngagementId,
    ass.Assessment_TenantVendorId,
    pq.ProvisionQuestion_AuthorityProvisionId,  -- ProvisionID
    qa.Answer_QuestionId,
    qa.Answer_Id,
    qa.Question_TypeCode,
    qa.Question_Weighting,
    qa.Question_Multiplier,
    qa.Answer_RiskStatusCode,
    qa.Answer_AnswerText,
    qa.Answer_IsCompleted,
    case
        when ass.Assessment_QuestionTypeCode = 'Risk Rated' then qa.Answer_RiskStatusCalc * 1.0
    end Answer_RiskStatusCalc,
    case
        when ass.Assessment_QuestionTypeCode = 'Weighted Score' then qa.Answer_WeightedScore * 1.0
    end Answer_WeightedScore,
    /******************** 
    RiskStatus is the RiskStatus stored in DB and ComponentStr
    RiskStatusCalc is the conversion to an ordered range 0,1,2,3,4,5 for rolling up the Risk Status measures
    WeightedScore is the Weighting column in Question table X Selected Answer Multiplier in 'rank' attribute of Question ComponentStr 
*********************/
    case
        when ass.Assessment_QuestionTypeCode = 'Risk Rated'
        then qa.Answer_RiskStatusCalc * 1.0
        when ass.Assessment_QuestionTypeCode = 'Weighted Score'
        then qa.Answer_WeightedScore * 1.0
        else 0 * 1.0
    end Answer_Score
from {{ ref("vwAssessmentTemplate") }} tpl
join
    {{ ref("vwAssessment") }} ass
    on ass.Assessment_CreatedFromTemplateId = tpl.Template_Id
    and tpl.Template_IsArchived = 0
    and tpl.Template_Status in (3, 100)
    and ass.Assessment_IsArchived = 0
    and ass.Assessment_IsDeprecatedAssessmentVersion = 0
    and ass.Assessment_Status in (4, 5, 6)  -- completed
join {{ ref("vwAssessmentDomain") }} ad on ad.AssessmentDomain_AssessmentId = ass.Assessment_Id
join {{ ref("vwQuestionAnswerMulti") }} qa on qa.Question_AssessmentDomainId = ad.AssessmentDomain_ID
join {{ ref("vwProvisionQuestion") }} pq on pq.ProvisionQuestion_QuestionId = qa.Answer_QuestionId

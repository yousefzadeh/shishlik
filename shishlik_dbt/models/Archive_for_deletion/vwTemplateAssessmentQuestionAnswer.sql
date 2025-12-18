-- Template to Assessment to Question and Answer
-- Used for Authority to Assessment Bar Chart with mapping
select
    asst.Template_AuthorityId Authority_Id,
    asst.Template_Id,
    asst.Template_Name,
    asst.Template_TemplateVersion,
    asst.Template_QuestionTypeCode,
    asst.Template_TenantId,
    ass.Assessment_Id,
    ass.Assessment_QuestionTypeCode QuestionType,
    assd.AssessmentDomain_Name,
    pq.ProvisionQuestion_AuthorityProvisionId AuthorityProvision_Id,
    qa.Answer_QuestionId Question_Id,
    qa.Answer_Id,
    qa.Answer_AnswerText ActualResponse
from {{ ref("vwAssessmentTemplate") }} asst
join {{ ref("vwAssessment") }} ass on asst.Template_ID = ass.Assessment_CreatedFromTemplateId
join {{ ref("vwAssessmentDomain") }} assd on ass.Assessment_ID = assd.AssessmentDomain_AssessmentId
join {{ ref("vwQuestionAnswerMulti") }} qa on assd.AssessmentDomain_ID = qa.Question_AssessmentDomainId
join {{ ref("vwProvisionQuestion") }} pq on qa.Answer_QuestionId = pq.ProvisionQuestion_QuestionId

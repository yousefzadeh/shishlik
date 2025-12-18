-- Authority and Provision joined to Assessment and Questions 
select
    'Provision linked to Question' provision_part,
    auth_ass.Tenant_Id,
    auth_ass.part Assessment_part,
    prov.Authority_Id,
    prov.Authority_Name,
    auth_ass.Provision_Id,
    prov.Provision_ReferenceId,
    auth_ass.Assessment_Id,
    auth_question.part Question_part,
    auth_question.question_Id,
    qa.Question_IdRef,
    qa.Question_Status,
    qa.part Answer_part,
    qa.AnswerResponse_PK,
    qa.Answer_Compliance,
    qa.Answer_ResponseCount,
    qa.Answer_Score,
    qa.Answer_RiskStatusCode,
    qa.Answer_RiskStatusCalc,
    qa.Answer_TextArea,
    qa.AnswerResponse_Value,
    auth_ass.link_count assessment_link_count,
    auth_question.link_count question_link_count
from {{ ref("fact_provision_assessment_relation") }} auth_ass
join
    {{ ref("fact_provision_question_relation") }} auth_question
    on auth_ass.Tenant_Id = auth_question.Tenant_Id
    and auth_ass.Provision_Id = auth_question.Provision_Id
    and auth_ass.Assessment_Id = auth_question.Assessment_Id
join
    {{ ref("vwQuestionOptionAnswerResponse_V3") }} qa
    on auth_question.Question_Id = qa.Question_Id
    and auth_question.Tenant_Id = qa.Answer_TenantId
join
    {{ ref("dim_provision") }} prov on auth_ass.Provision_Id = prov.Provision_Id and auth_ass.Tenant_Id = prov.Tenant_Id
--
union all
--
-- Authority and Provision Linked to Assessment but unlinked to Questions 
select
    'Provision unlinked to Question' provision_part,
    auth_ass.Tenant_Id,
    auth_ass.part Assessment_part,
    prov.Authority_Id,
    prov.Authority_Name,
    auth_ass.Provision_Id,
    prov.Provision_ReferenceId,
    auth_ass.Assessment_Id,
    'Unassigned Question' Question_part,
    0 question_Id,
    NULL Question_IdRef,
    'Unassigned' Question_Status,
    'Unassigned Question' Answer_part,
    NULL AnswerResponse_PK,
    NULL Answer_Compliance,
    0 Answer_ResponseCount,
    0 Answer_Score,
    NULL Answer_RiskStatusCode,
    0 Answer_RiskStatusCalc,
    'Blank because Provision is not linked to any Question in this Assessment' Answer_TextArea,
    'Not linked to any question' AnswerResponse_Value,
    auth_ass.link_count assessment_link_count,
    0 question_link_count
from {{ ref("fact_provision_assessment_relation") }} auth_ass
left join
    {{ ref("fact_provision_question_relation") }} auth_question
    on auth_ass.Tenant_Id = auth_question.Tenant_Id
    and auth_ass.Provision_Id = auth_question.Provision_Id
    and auth_ass.Assessment_Id = auth_question.Assessment_Id
join
    {{ ref("dim_provision") }} prov on auth_ass.Provision_Id = prov.Provision_Id and auth_ass.Tenant_Id = prov.Tenant_Id
where auth_question.Tenant_Id is NULL

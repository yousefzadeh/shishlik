/*
  Fact table for Authority To Assessment Report

  Union of 2 fact tables
  1. Provisions linked to Questions linked to Target Authorities and Provision
  2. Provisions not linked to any Questions linked to Target Authorities and Provision

 */
with
    map as (
        select SourceAuthorityProvisionId, TargetAuthorityProvisionId, TenantId
        from {{ source("tenant_models", "TenantAuthorityProvisionMapping") }}
    ),
    linked_auth_question as (
        select
            auth_ass.Tenant_Id,
            auth_ass.part Assessment_part,
            auth_ass.Provision_Id,
            auth_ass.Assessment_Id,
            auth_question.part Question_part,
            auth_question.Domain_Name,
            auth_question.question_Id,
            map.TargetAuthorityProvisionId,
            auth_ass.link_count assessment_link_count,
            auth_question.link_count question_link_count
        from {{ ref("fact_provision_assessment_relation_hash_join") }} auth_ass inner hash
        join
            {{ ref("fact_provision_question_relation_hash_join") }} auth_question
            on auth_ass.Tenant_Id = auth_question.Tenant_Id
            and auth_ass.Provision_Id = auth_question.Provision_Id
            and auth_ass.Assessment_Id = auth_question.Assessment_Id
            inner hash
        join
            map
            on auth_question.Provision_Id = map.SourceAuthorityProvisionId
            and auth_question.Tenant_Id = map.TenantId
    ),
    unlinked_auth_question as (
        select
            auth_ass.Tenant_Id,
            auth_ass.part Assessment_part,
            auth_ass.Provision_Id,
            auth_ass.Assessment_Id,
            map.TargetAuthorityProvisionId,
            auth_ass.link_count assessment_link_count
        from {{ ref("fact_provision_assessment_relation_hash_join") }} auth_ass inner hash
        join map on auth_ass.Provision_Id = map.SourceAuthorityProvisionId and auth_ass.Tenant_Id = map.TenantId
        where
            not exists (
                select 1
                from {{ ref("fact_provision_question_relation_hash_join") }} auth_question
                where
                    auth_ass.Tenant_Id = auth_question.Tenant_Id
                    and auth_ass.Provision_Id = auth_question.Provision_Id
                    and auth_ass.Assessment_Id = auth_question.Assessment_Id
            )
    ),
    linked_fact as (
        -- Authority and Provision joined to Assessment and Questions 
        select
            'Provision linked to Question' provision_part,
            auth_question.Tenant_Id,  -- access tenant
            auth_question.Assessment_part,
            prov.AuthorityId Authority_Id,
            auth.Name Authority_Name,  -- source authority
            auth.Name Authority_NameLabel,  -- source authority
            auth_question.Provision_Id,  -- source provision
            target_prov.AuthorityId TargetAuthority_Id,  -- target authority
            target_auth.Name TargetAuthority_Name,  -- target authority
            target_auth.Name TargetAuthority_NameLabel,  -- target authority
            target_prov.Id TargetProvision_Id,  -- target provision
            target_prov.Name TargetProvision_Name,  -- target provision
            target_prov.ReferenceId TargetProvision_ReferenceId,  -- target provision
            custom.ProvisionCustom_FieldName TargetProvision_FieldName,  -- target provision
            custom.ProvisionCustom_Value TargetProvision_Value,  -- target provision
            auth_question.Assessment_Id,
            ass.Template_Name,  -- template
            ass.Assessment_Name,  -- assessment
            ass.Assessment_IsLatest,
            ass.Assessment_Version,
            ass.Assessment_QuestionType,
            auth_question.Question_part,
            auth_question.Domain_Name,
            auth_question.question_Id,
            qa.Question_Name,
            qa.Question_IdRef,
            qa.Question_Status,
            qa.part Answer_part,
            qa.AnswerResponse_PK,
            qa.Answer_Compliance,
            qa.Answer_ResponseCount,
            qa.Answer_Score,
            coalesce(qa.Answer_RiskStatusCode, 'Unassigned Risk Rating') Answer_RiskStatusCode,
            qa.Answer_RiskStatusCalc,
            qa.Answer_TextArea,
            coalesce(qa.AnswerResponse_Value, 'Blank') AnswerResponse_Value,
            auth_question.assessment_link_count,
            auth_question.question_link_count
        from linked_auth_question auth_question inner hash
        join
            {{ ref("vwQuestionOptionAnswerResponse_V3") }} qa
            on auth_question.Question_Id = qa.Question_Id
            and auth_question.Tenant_Id = qa.Answer_TenantId
            inner hash
        join
            {{ ref("fact_provision_custom_field_value_hash_join") }} custom  -- target provision
            on auth_question.TargetAuthorityProvisionId = custom.Provision_Id
            and auth_question.Tenant_Id = custom.Tenant_Id
            inner hash
        join
            {{ source("assessment_models", "AuthorityProvision") }} prov on auth_question.Provision_Id = prov.Id
            inner hash
        join {{ source("assessment_models", "Authority") }} auth on auth.Id = prov.AuthorityId inner hash
        join
            {{ source("assessment_models", "AuthorityProvision") }} target_prov
            on auth_question.TargetAuthorityProvisionId = target_prov.Id
            inner hash
        join
            {{ source("assessment_models", "Authority") }} target_auth on target_auth.Id = target_prov.AuthorityId
            inner hash
        join {{ ref("dim_assessment") }} ass on auth_question.Assessment_Id = ass.Assessment_Id
    ),
    unlinked_fact as (
        -- Authority and Provision Linked to Assessment but unlinked to Questions 
        select
            'Provision unlinked to Question' provision_part,
            auth_question.Tenant_Id,
            auth_question.Assessment_part,
            prov.AuthorityId Authority_Id,  -- source authority
            auth.Name Authority_Name,  -- source authority
            auth.Name Authority_NameLabel,  -- source authority
            auth_question.Provision_Id,  -- source provision
            target_prov.AuthorityId TargetAuthority_Id,  -- target authority
            target_auth.Name TargetAuthority_Name,  -- target authority
            target_auth.Name TargetAuthority_NameLabel,  -- target authority
            target_prov.Id TargetProvision_Id,  -- target provision
            target_prov.Name TargetProvision_Name,  -- target provision
            target_prov.ReferenceId TargetProvision_ReferenceId,  -- target provision
            custom.ProvisionCustom_FieldName TargetProvision_FieldName,  -- target provision
            custom.ProvisionCustom_Value TargetProvision_Value,  -- target provision
            auth_question.Assessment_Id,
            ass.Template_Name,
            ass.Assessment_Name,
            ass.Assessment_IsLatest,
            ass.Assessment_Version,
            ass.Assessment_QuestionType,
            'Unassigned Question' Question_part,
            'Unassigned Domain' Domain_Name,
            0 question_Id,
            'Blank' Question_Name,
            NULL Question_IdRef,
            'Not linked to any question' Question_Status,
            'Unassigned Question' Answer_part,
            NULL AnswerResponse_PK,
            NULL Answer_Compliance,
            0 Answer_ResponseCount,
            0 Answer_Score,
            'Not linked to any question' Answer_RiskStatusCode,
            0 Answer_RiskStatusCalc,
            'Blank because Provision is not linked to any Question in this Assessment' Answer_TextArea,
            'Not linked to any question' AnswerResponse_Value,
            auth_question.assessment_link_count,
            1 question_link_count  -- count unlinked provisions for both Weighted and Risk rated assessments
        from unlinked_auth_question auth_question inner hash
        join
            {{ ref("fact_provision_custom_field_value_hash_join") }} custom  -- target provision
            on auth_question.TargetAuthorityProvisionId = custom.Provision_Id
            and auth_question.Tenant_Id = custom.Tenant_Id
            inner hash
        join
            {{ source("assessment_models", "AuthorityProvision") }} prov on auth_question.Provision_Id = prov.Id
            inner hash
        join {{ source("assessment_models", "Authority") }} auth on auth.Id = prov.AuthorityId inner hash
        join
            {{ source("assessment_models", "AuthorityProvision") }} target_prov
            on auth_question.TargetAuthorityProvisionId = target_prov.Id
            inner hash
        join
            {{ source("assessment_models", "Authority") }} target_auth on target_auth.Id = target_prov.AuthorityId
            inner hash
        join {{ ref("dim_assessment") }} ass on auth_question.Assessment_Id = ass.Assessment_Id
    ),
    uni as (
        select *
        from linked_fact
        union all
        select *
        from unlinked_fact
    )
select
    provision_part,
    Tenant_Id,
    Assessment_part,
    Authority_Id,
    Authority_Name,
    Authority_NameLabel,
    Provision_Id,
    TargetAuthority_Id,  -- target authority
    TargetAuthority_Name,  -- target authority
    TargetAuthority_NameLabel,  -- target authority
    TargetProvision_Id,  -- target provision
    TargetProvision_Name,  -- target provision
    TargetProvision_ReferenceId,  -- target provision
    TargetProvision_FieldName,  -- target provision
    TargetProvision_Value,  -- target provision
    Assessment_Id,
    Template_Name,
    Assessment_Name,
    Assessment_IsLatest,
    Assessment_Version,
    case
        when Assessment_QuestionType = 0
        then 'Preferred Answer'
        when Assessment_QuestionType = 1
        then 'Weighted Score'
        when Assessment_QuestionType = 2
        then 'Risk Rated'
        else 'Undefined'
    end Assessment_QuestionType,
    Question_part,
    Domain_Name,
    question_Id,
    Question_Name,
    Question_IdRef,
    Question_Status,
    Answer_part,
    AnswerResponse_PK,
    Answer_Compliance,
    Answer_ResponseCount,
    case when Assessment_QuestionType = 1 then Answer_Score end Answer_Score,
    case when Assessment_QuestionType = 2 then Answer_RiskStatusCode else 'Not Risk Rated' end Answer_RiskStatusCode,
    case when Assessment_QuestionType = 2 then Answer_RiskStatusCalc end Answer_RiskStatusCalc,
    Answer_TextArea,
    AnswerResponse_Value,
    assessment_link_count,
    question_link_count
from uni
where Assessment_IsLatest = 1

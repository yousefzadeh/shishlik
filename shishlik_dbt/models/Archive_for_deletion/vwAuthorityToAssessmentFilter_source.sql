{{- config(materialized="view") -}}
with
    source as (
        select
            auth.Authority_Id,
            auth.Authority_Name,
            asqa.Template_Name,
            asqa.Assessment_Id,
            asqa.Assessment_Name,
            prov.AuthorityProvision_Id,
            prov.AuthorityProvision_Name,
            dcfv.AuthorityCustom_FieldName,
            asqa.Assessment_TenantId,
            {# greatest(
      asqa.AssessmentQuestionAnswer_UpdateTime,
      dp_id.QuestionProvision_UpdateTime,
      dcfv.ProvisionCustomFieldValue_UpdateTime,
      auth.Authority_UpdateTime,
      prov.AuthorityProvision_UpdateTime
    ) Filter_UpdateTime, #}
            {{
                safe_concat(
                    [
                        "auth.Authority_Id",
                        "asqa.Template_Name",
                        "asqa.Assessment_Id",
                        "prov.AuthorityProvision_Id",
                        "dcfv.AuthorityCustom_FieldName",
                        "asqa.Assessment_TenantId",
                    ]
                )
            }} Filter_PK
        from {{ ref("vwAuthorityAssessmentQuestionAnswer_lambda") }} as asqa
        inner join
            {{ ref("vwDirectQuestionProvisionAuthority_source") }} as dp_id
            on asqa.Answer_QuestionId = dp_id.ProvisionQuestion_QuestionId
        inner join
            {{ ref("vwProvisionCustomFieldValue_lambda") }} as dcfv
            on dp_id.Direct_AuthorityId = dcfv.AuthorityCustom_AuthorityId
            and dp_id.Direct_AuthorityProvisionId = dcfv.Provision_Id
        inner join {{ ref("vwAuthorityZero_source") }} as auth on asqa.Assessment_AuthorityId = auth.Authority_Id
        inner join
            {{ ref("vwAuthorityProvisionZero_source") }} as prov
            on dp_id.Direct_AuthorityProvisionId = prov.AuthorityProvision_Id
    ),
    final as (
        select
            Authority_Name,
            Template_Name,
            Assessment_Name,
            AuthorityProvision_Name,
            AuthorityCustom_FieldName,
            Assessment_TenantId,
            Filter_PK
        from source
        group by
            Authority_Name,
            Template_Name,
            Assessment_Name,
            AuthorityProvision_Name,
            AuthorityCustom_FieldName,
            Assessment_TenantId,
            Filter_PK
    )
select *
from final

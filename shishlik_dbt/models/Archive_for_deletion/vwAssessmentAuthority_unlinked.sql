{{- config(materialized="view") -}}
-- Assessment with no links to Authority or ControlSet - 1 to many
with
    ass_temp as (
        select
            Assessment_Id,
            Assessment_Name,
            Assessment_AssessmentVersion,
            Assessment_TenantId,
            Assessment_AuthorityId,
            Assessment_PolicyId,
            Assessment_CreatedFromTemplateId,
            Assessment_StatusCode,
            Assessment_QuestionTypeCode,
            Assessment_WorkFlow,
            Template_Id,
            Template_Name
        from {{ ref("vwAssessmentTemplate_union") }}
    ),
    ass_temp_no_auth as (
        select
            Assessment_Id,
            Assessment_Name,
            Assessment_AssessmentVersion,
            Assessment_TenantId,
            0 Assessment_AuthorityId,
            Assessment_StatusCode,
            Assessment_QuestionTypeCode,
            Assessment_WorkFlow,
            Template_Id,
            Template_Name
        from ass_temp
        where Assessment_AuthorityId is NULL and Assessment_PolicyId is NULL
    ),
    final as (select 'No Authority' relation, ass_temp_no_auth.* from ass_temp_no_auth)
select *
from final

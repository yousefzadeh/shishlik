{{- config(materialized="view") -}}
-- Assessment and Mapping to Authority - 1 to many
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
    ass_temp_auth as (
        select
            Assessment_Id,
            Assessment_Name,
            Assessment_AssessmentVersion,
            Assessment_TenantId,
            Assessment_AuthorityId,
            Assessment_StatusCode,
            Assessment_QuestionTypeCode,
            Assessment_WorkFlow,
            Template_Id,
            Template_Name
        from ass_temp
        where Assessment_AuthorityId is not null
    ),
    final as (select 'Authority' relation, ass_temp_auth.* from ass_temp_auth)
select *
from final

{{- config(materialized="view") -}}
-- Assessment and Mapping to ControlSet to Authority - 1 to many
with
    ass_temp as (
        select
            Assessment_Id,
            Assessment_Name,
            Assessment_AssessmentVersion,
            Assessment_TenantId,
            Assessment_AuthorityId,
            Assessment_PolicyId,
            Assessment_StatusCode,
            Assessment_QuestionTypeCode,
            Assessment_WorkFlow,
            Template_Id,
            Template_Name
        from {{ ref("vwAssessmentTemplate_union") }}
    ),
    policy_auth as (
        select pol.Policy_Id, apol.AuthorityPolicy_AuthorityId
        from {{ ref("vwPolicy") }} pol
        join {{ ref("vwAuthorityPolicy") }} apol on pol.Policy_Id = apol.AuthorityPolicy_PolicyId
        union all
        select 0 Policy_Id, -1 AuthorityPolicy_AuthorityId
    ),
    ass_temp_policy_auth as (
        select
            Assessment_Id,
            Assessment_Name,
            Assessment_AssessmentVersion,
            Assessment_TenantId,
            policy_auth.AuthorityPolicy_AuthorityId Assessment_AuthorityId,
            Assessment_StatusCode,
            Assessment_QuestionTypeCode,
            Assessment_WorkFlow,
            Template_Id,
            Template_Name
        from ass_temp
        join policy_auth on ass_temp.Assessment_PolicyId = policy_auth.Policy_Id
    ),
    final as (select 'ControlSet Authority' relation, ass_temp_policy_auth.* from ass_temp_policy_auth)
select *
from final

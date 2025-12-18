{{- config(materialized="view") -}}
-- Assessment and Mapping to Authority - 1 to many
with
    ass as (  -- Only Assessments not deleted and not archived
        select
            Assessment_Id,
            Assessment_Name,
            Assessment_AssessmentVersion,
            Assessment_AssessmentVersionName,
            Assessment_Name_Responding_Team,
            Assessment_TenantId,
            coalesce(Assessment_AuthorityId, 0) Assessment_AuthorityId,
            coalesce(Assessment_PolicyId, 0) Assessment_PolicyId,
            Assessment_CreatedFromTemplateId,  -- Created without Template = -1
            Assessment_Status,
            Assessment_StatusCode,
            Assessment_TenantVendorId,
            Assessment_QuestionTypeCode,
            Assessment_WorkFlow,
            Assessment_IsCurrent
        from {{ ref("vwAssessment") }} ass
        where
            Assessment_Status = 4  -- StatusCode = 'Completed'
            and ass.Assessment_IsArchived = 0
    ),
    temp_union as (
        {#
      from vwAssessment  
     ,case when IsArchived = 1 
      then 'No Template' 
      else a.[Name] end [Name]
     ,a.[Name] + ' ('+ tv.TenantVendor_Name + ')' AS Name_Responding_Team
      
    #}
        -- Templates taken from dbo for all rows to take care of deleted cases
        -- IsArchived 0/1 is taken care of by union all for query optimizer to get accuracte cardinality
        select asst.Id Template_Id, asst.Name Template_Name  -- Templates not archived and not deleted
        from {{ source("assessment_models", "Assessment") }} asst
        where IsTemplate = 1 and IsArchived = 0 and IsDeleted = 0
        union all
        select  -- Deleted Templates
            asst.Id Template_Id, 'No Template' Template_Name
        from {{ source("assessment_models", "Assessment") }} asst
        where IsTemplate = 1 and IsArchived in (0, 1) and IsDeleted = 1
        union all
        select  -- Archived templates
            asst.Id Template_Id, 'No Template' Template_Name
        from {{ source("assessment_models", "Assessment") }} asst
        where IsTemplate = 1 and IsArchived = 1 and IsDeleted in (0, 1)
        union all
        select  -- Created with no Template
            -1 Template_id,  -- Check in vwAssessment coalesce(CreatedFromTemplateId,-1)
            'No Template' Template_Name
    ),
    temp as (select Template_Id, max(Template_Name) Template_Name from temp_union group by Template_Id),
    ass_temp as (
        select ass.*, temp.Template_Name from ass join temp on ass.Assessment_CreatedFromTemplateId = temp.Template_Id
    ),
    policy_auth as (
        select pol.Policy_Id, apol.AuthorityPolicy_AuthorityId
        from {{ ref("vwPolicy") }} pol
        join {{ ref("vwAuthorityPolicy") }} apol on pol.Policy_Id = apol.AuthorityPolicy_PolicyId
        union all
        select 0 Policy_Id, -1 AuthorityPolicy_AuthorityId
    ),
    ass_temp_auth as (
        select
            Assessment_Id,
            Assessment_Name,
            Assessment_AssessmentVersion,
            Assessment_AssessmentVersionName,
            Assessment_Name_Responding_Team,
            Assessment_TenantId,
            Assessment_AuthorityId,
            Assessment_StatusCode,
            Assessment_TenantVendorId,
            Assessment_QuestionTypeCode,
            Assessment_WorkFlow,
            Assessment_IsCurrent,
            Template_Name
        from ass_temp
    ),
    ass_temp_policy_auth as (
        select
            Assessment_Id,
            Assessment_Name,
            Assessment_AssessmentVersion,
            Assessment_AssessmentVersionName,
            Assessment_Name_Responding_Team,
            Assessment_TenantId,
            policy_auth.AuthorityPolicy_AuthorityId Assessment_AuthorityId,
            Assessment_StatusCode,
            Assessment_TenantVendorId,
            Assessment_QuestionTypeCode,
            Assessment_WorkFlow,
            Assessment_IsCurrent,
            Template_Name
        from ass_temp
        join policy_auth on ass_temp.Assessment_PolicyId = policy_auth.Policy_Id
    ),
    final as (
        select
            'Authority' relation,
            Assessment_Id,
            Assessment_Name,
            Assessment_AssessmentVersion,
            Assessment_AssessmentVersionName,
            Assessment_Name_Responding_Team,
            Assessment_TenantId,
            Assessment_AuthorityId,
            Assessment_StatusCode,
            Assessment_TenantVendorId,
            Assessment_QuestionTypeCode,
            Assessment_WorkFlow,
            Assessment_IsCurrent,
            Template_Name
        from ass_temp_auth
        where Assessment_AuthorityId > 0
        union all
        select
            'ControlSet Authority' relation,
            Assessment_Id,
            Assessment_Name,
            Assessment_AssessmentVersion,
            Assessment_AssessmentVersionName,
            Assessment_Name_Responding_Team,
            Assessment_TenantId,
            Assessment_AuthorityId,
            Assessment_StatusCode,
            Assessment_TenantVendorId,
            Assessment_QuestionTypeCode,
            Assessment_WorkFlow,
            Assessment_IsCurrent,
            Template_Name
        from ass_temp_policy_auth
        where Assessment_AuthorityId > 0
    )
select *
from final

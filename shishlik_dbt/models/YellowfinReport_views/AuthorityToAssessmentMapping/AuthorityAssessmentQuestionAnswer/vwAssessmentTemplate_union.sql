{{- config(materialized="view") -}}
-- Assessment and Mapping to Authority - 1 to many
with
    ass as (  -- Only Assessments not deleted and not archived
        select
            Id Assessment_Id,
            NameVarChar Assessment_Name,
            AssessmentVersion Assessment_AssessmentVersion,
            TenantId Assessment_TenantId,
            AuthorityId Assessment_AuthorityId,  -- Nullable
            PolicyId Assessment_PolicyId,  -- Nullable
            CreatedFromTemplateId Assessment_CreatedFromTemplateId,  -- Nullable
            'Completed' Assessment_StatusCode,
            case
                when [QuestionType] = 0
                then 'Preferred Answer'
                when [QuestionType] = 1
                then 'Weighted Score'
                when [QuestionType] = 2
                then 'Risk Rated'
                else 'Undefined'
            end Assessment_QuestionTypeCode,
            case
                when [WorkFlowId] = 1 then 'Requirement' when [WorkFlowId] = 0 then 'Question' else 'Undefined'
            end Assessment_WorkFlow
        from {{ source("assessment_models", "Assessment") }} ass
        where
            Status = 4  -- StatusCode = 'Completed'
            and ass.IsDeleted = 0
            and ass.IsArchived = 0
            and IsDeprecatedAssessmentVersion = 0
            and IsTemplate = 0
    ),
    temp_union as (
        {#
      from vwAssessment  
     ,case when IsArchived = 1 
      then 'No Template' 
      else cast(a.[Name] as varchar(200)) end [Name]
     ,cast(a.[Name] + ' ('+ tv.TenantVendor_Name + ')' as varchar(200)) Name_Responding_Team
      
    #}
        -- Templates taken from dbo for all rows to take care of deleted cases
        -- IsArchived 0/1 is taken care of by union all for query optimizer to get accuracte cardinality
        select asst.Id Template_Id, asst.NameVarChar Template_Name  -- Templates not archived and not deleted
        from {{ source("assessment_models", "Assessment") }} asst
        where IsTemplate = 1 and IsArchived = 0 and IsDeleted = 0
        union all
        select  -- Deleted Templates
            asst.Id Template_Id, concat(asst.Name, '(Deleted)') Template_Name
        from {{ source("assessment_models", "Assessment") }} asst
        where IsTemplate = 1 and IsArchived in (0, 1) and IsDeleted = 1
        union all
        select  -- Archived templates
            asst.Id Template_Id, concat(asst.Name, '(Archived)') Template_Name
        from {{ source("assessment_models", "Assessment") }} asst
        where IsTemplate = 1 and IsArchived = 1 and IsDeleted in (0, 1)
        union all
        select  -- Created with no Template
            -1 Template_id,  -- Check in vwAssessment coalesce(CreatedFromTemplateId,-1)
            'No Template' Template_Name
    ),
    temp as (select Template_Id, max(Template_Name) Template_Name from temp_union group by Template_Id),
    ass_temp as (
        select ass.*, temp.Template_Id, temp.Template_Name
        from ass
        join temp on ass.Assessment_CreatedFromTemplateId = temp.Template_Id
    )
select *
from ass_temp

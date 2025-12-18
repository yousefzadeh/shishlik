-- one row per Assessment
-- zero - no Assessment assign no filter on Tenant Assessment_Id = 0
-- zero - no Assessment assigned at each Tenant Assessment_Id = -Tenant_Id
with
    ass as (
        select
            Id Assessment_Id,
            [NameVarchar] Assessment_Name,
            IsDeleted Assessment_IsDeleted,
            IsArchived Assessment_IsArchived,
            IsDeprecatedAssessmentVersion Assessment_IsDeprecatedVersion,
            AssessmentVersion Assessment_Version,
            [Status] Assessment_Status,
            CreatedFromTemplateId,
            QuestionType Assessment_QuestionType,
            TenantId Assessment_TenantId
        from {{ source("assessment_models", "Assessment") }}
        where IsTemplate = 0
    ),
    template as (
        select Id Template_Id, [Name] Template_Name
        from {{ source("assessment_models", "Assessment") }}
        where IsTemplate = 1 and IsDeleted = 0 and IsArchived = 0
    ),
    ass_template as (
        select
            ass.Assessment_Id,
            ass.Assessment_Name,
            ass.Assessment_IsDeleted,
            ass.Assessment_IsArchived,
            1 - ass.Assessment_IsDeprecatedVersion Assessment_IsLatest,
            ass.Assessment_Status,
            ass.Assessment_Version,
            ass.Assessment_QuestionType,
            template.Template_Name Template_Name,
            ass.Assessment_TenantId
        from ass
        join template on ass.CreatedFromTemplateId = Template_Id
        where ass.Assessment_IsDeleted = 0 and ass.Assessment_IsArchived = 0 and ass.Assessment_Status in (4, 5, 6)
    ),
    ass_no_template as (
        select
            ass.Assessment_Id,
            ass.Assessment_Name,
            ass.Assessment_IsDeleted,
            ass.Assessment_IsArchived,
            1 - ass.Assessment_IsDeprecatedVersion Assessment_IsLatest,
            ass.Assessment_Status,
            ass.Assessment_Version,
            ass.Assessment_QuestionType,
            'No Template' Template_Name,
            ass.Assessment_TenantId
        from ass
        left join template on ass.CreatedFromTemplateId = Template_Id
        where
            ass.Assessment_IsDeleted = 0
            and ass.Assessment_IsArchived = 0
            and ass.Assessment_Status in (4, 5, 6)
            and template.Template_Id is NULL
    ),
    final as (
        select 'created from Template' part, *
        from ass_template
        union all
        select 'created without Template' part, *
        from ass_no_template
    )
select
    Assessment_Id,
    Assessment_Name,
    Assessment_IsLatest,
    case
        Assessment_Status
        when 1
        then 'Draft'
        when 2
        then 'Approved'
        when 3
        then 'Published'
        when 4
        then 'Completed'
        when 5
        then 'Closed'
        when 6
        then 'Reviewed'
        when 7
        then 'In Progress'
        when 8
        then 'Cancelled'
        else 'Undefined'
    end as Assessment_StatusCode,
    Assessment_Version,
    Template_Name,
    Assessment_QuestionType,
    case
        when Assessment_QuestionType = 0
        then 'Preferred Answer'
        when Assessment_QuestionType = 1
        then 'Weighted Score'
        when Assessment_QuestionType = 2
        then 'Risk Rated'
        else 'Undefined'
    end QuestionTypeCode,
    Assessment_TenantId
from final

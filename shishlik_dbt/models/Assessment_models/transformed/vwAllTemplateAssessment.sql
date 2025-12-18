{# 
DOC START
  - name: vwAllTemplateAssessment
    description: |
      One row for both Question (QBA) and Requirements Based Assessments (RBA) Template and Assessment.
    columns:
      - name: Template_Id

      - name: Template_Name

      - name: Template_Status

      - name: Template_StatusCode

      - name: Template_PublishedDate

      - name: Template_PublishedById

      - name: Assessment_ID

      - name: Assessment_TenantId

      - name: Assessment_RespondingTeam

      - name: Assessment_Name

      - name: Assessment_Name_RespondingTeam

      - name: Assessment_Status

      - name: Assessment_StatusCode

      - name: Assessment_Objective

      - name: Assessment_Description

      - name: Assessment_Tags

      - name: Assessment_DueDate

      - name: Assessment_OverdueFlag

      - name: Assessment_EngagementId

      - name: Assessment_PolicyId

      - name: Assessment_PublishedDate

      - name: Assessment_TypeId

      - name: Assessment_QuestionType

      - name: Assessment_QuestionTypeCode

      - name: Assessment_RoundResultToNearest

      - name: Assessment_ACROSSDomain

      - name: Assessment_WithinDomain

      - name: Assessment_ParentTemplateId

      - name: Assessment_RootTemplateId

      - name: Assessment_TemplateVersion

      - name: Assessment_HasPeriod

      - name: Assessment_Period

      - name: Assessment_PeriodCode

      - name: Assessment_PeriodStartDate

      - name: Assessment_IsArchived

      - name: Assessment_ImageUrl

      - name: Assessment_IsArchivedForVendor

      - name: Assessment_ReAssessmentParentId

      - name: Assessment_ReAssessmentRootId

      - name: Assessment_ReAssessmentVersion

      - name: Assessment_AuthorityId

      - name: Assessment_PublishedById

      - name: Assessment_ArchivedDate

      - name: Assessment_ClosedDate

      - name: Assessment_ClosedReason

      - name: Assessment_ResponseCompletedDate
 
      - name: Assessment_ResponseStartedDate

      - name: Assessment_ReviewedDate

      - name: Assessment_AssessmentVersion

      - name: Assessment_ParentAssessmentId

      - name: Assessment_RootAssessmentId

      - name: Assessment_IsDeprecatedAssessmentVersion

      - name: Assessment_CreatedFromAssessmentId

      - name: Assessment_WorkFlowId

      - name: Assessment_WorkFlow

      - name: Assessment_TenantVendorId

      - name: Assessment_IsCurrent

      - name: Assessment_CreationTime

      - name: Assessment_UpdateTime

DOC END
#}
{{ config(materialized="view") }}
with
    template as (
        select
            Id Template_Id,
            case
                when IsArchived = 1
                then 'No Template (Archived)'
                when IsDeleted = 1
                then 'No Template (Deleted)'
                else cast(a.Name as varchar(200))
            end as Template_Name,
            Status Template_Status,
            case
                when Status = 1
                then 'Draft'
                when Status = 2
                then 'Approved'
                when Status = 3
                then 'Published'
                when Status = 4
                then 'Completed'
                when Status = 5
                then 'Closed'
                when Status = 6
                then 'Reviewed'
                when Status = 7
                then 'In Progress'
                when Status = 8
                then 'Cancelled'
                when Status = 100
                then 'Deprecated'
                else 'Undefined'
            end as Template_StatusCode,
            PublishedDate Template_PublishedDate,
            PublishedById Template_PublishedById
        from {{ source("assessment_models", "Assessment") }} a
        where IsTemplate = 1 and IsDeleted = 0
    ),
    assessment as (
        select
            Id,
            TenantId,
            Name,
            tv.TenantVendor_Name RespondingTeam,
            cast(Name + ' (' + tv.TenantVendor_Name + ')' as varchar(200)) Name_RespondingTeam,
            Status,
            case
                when Status = 1
                then 'Draft'
                when Status = 2
                then 'Approved'
                when Status = 3
                then 'Published'
                when Status = 4
                then 'Completed'
                when Status = 5
                then 'Closed'
                when Status = 6
                then 'Reviewed'
                when Status = 7
                then 'In Progress'
                when Status = 8
                then 'Cancelled'
                when Status = 100 
                then 'Deprecated'
                else 'Undefined'
            end as StatusCode,
            cast(Objective as nvarchar(4000)) Objective,
            cast(Description as nvarchar(4000)) Description,
            cast(Tags as nvarchar(4000)) Tags,
            DueDate,
            case
                when Status in (4, 5, 6)
                then 'Not Overdue'
                when Status not in (4, 5, 6) and DueDate is null
                then 'No Due Date'
                when Status not in (4, 5, 6) and DueDate < getdate()
                then 'Overdue'
                else 'Not Overdue'
            end as OverdueFlag,
            EngagementId,
            PolicyId,
            AuthorityId,
            PublishedDate,
            TypeId,
            QuestionType,
            case
                when QuestionType = 0
                then 'Preferred Answer'
                when QuestionType = 1
                then 'Weighted Score'
                when QuestionType = 2
                then 'Risk Rated'
                else 'Undefined'
            end QuestionTypeCode,
            RoundResultToNearest,
            ACROSSDomain,
            WithinDomain,
            ParentTemplateId,
            RootTemplateId,
            TemplateVersion,
            HasPeriod,
            Period,
            case
                Period
                when 1
                then 'One Year'
                when 2
                then 'Six Months'
                when 3
                then 'One Month'
                when 4
                then 'One Week'
                when 5
                then 'Five Minutes'
                when 6
                then 'Three Months'
                when 7
                then 'One Day'
                when 8
                then 'Two Years'
                when 9
                then 'Three Years'
                when 10
                then 'Two Weeks'
                else 'Unassigned'
            end PeriodCode,
            PeriodStartDate,
            IsArchived,
            cast(ImageUrl as nvarchar(4000)) ImageUrl,
            IsArchivedForVendor,
            ReAssessmentParentId,
            ReAssessmentRootId,
            ReAssessmentVersion,
            PublishedById,
            ArchivedDate,
            ClosedDate,
            cast(ClosedReason as nvarchar(4000)) ClosedReason,
            ResponseCompletedDate,
            ResponseStartedDate,
            ReviewedDate,
            AssessmentVersion,
            ParentAssessmentId,
            coalesce(RootAssessmentId, Id) RootAssessmentId,
            IsDeprecatedAssessmentVersion,
            CreatedFromAssessmentId,
            CreatedFromTemplateId,
            WorkFlowId,
            TenantVendorId,
            case
                when WorkFlowId = 1 then 'Requirement' when WorkFlowId = 0 then 'Question' else 'Undefined'
            end Workflow,
            case
                when
                    lead(CreationTime) over (partition by coalesce(RootAssessmentId, Id) order by TemplateVersion)
                    is null
                then 1
                else 0
            end IsCurrent,
            CreationTime,
            coalesce(LastModificationTime, CreationTime) UpdateTime
        from {{ source("assessment_models", "Assessment") }} a
        left join
            {{ ref("vwTenantVendor") }} tv
            on a.TenantVendorId = tv.TenantVendor_Id
            -- Note We need to determine the line item detail of this table.
        where IsTemplate = 0 and IsDeleted = 0
    ),
    ass as (
        select
            Template_Id,
            coalesce(Template_Name, 'No Template') Template_Name,
            Template_Status,
            Template_StatusCode,
            Template_PublishedDate,
            Template_PublishedById,
            {{ col_rename("ID", "Assessment") }},
            {{ col_rename("TenantId", "Assessment") }},
            {{ col_rename("RespondingTeam", "Assessment") }},

            {{ col_rename("Name", "Assessment") }},
            {{ col_rename("Name_RespondingTeam", "Assessment") }},
            {{ col_rename("Status", "Assessment") }},
            {{ col_rename("StatusCode", "Assessment") }},

            {{ col_rename("Objective", "Assessment") }},
            {{ col_rename("Description", "Assessment") }},
            {{ col_rename("Tags", "Assessment") }},
            {{ col_rename("DueDate", "Assessment") }},
            {{ col_rename("OverdueFlag", "Assessment") }},
            {{ col_rename("EngagementId", "Assessment") }},
            {{ col_rename("PolicyId", "Assessment") }},

            {{ col_rename("PublishedDate", "Assessment") }},

            {{ col_rename("TypeId", "Assessment") }},
            {{ col_rename("QuestionType", "Assessment") }},
            {{ col_rename("QuestionTypeCode", "Assessment") }},
            {{ col_rename("RoundResultToNearest", "Assessment") }},

            {{ col_rename("ACROSSDomain", "Assessment") }},
            {{ col_rename("WithinDomain", "Assessment") }},
            {{ col_rename("ParentTemplateId", "Assessment") }},
            {{ col_rename("RootTemplateId", "Assessment") }},

            {{ col_rename("TemplateVersion", "Assessment") }},

            {{ col_rename("HasPeriod", "Assessment") }},
            {{ col_rename("Period", "Assessment") }},
            {{ col_rename("PeriodCode", "Assessment") }},
            {{ col_rename("PeriodStartDate", "Assessment") }},

            {{ col_rename("IsArchived", "Assessment") }},
            {{ col_rename("ImageUrl", "Assessment") }},
            {{ col_rename("IsArchivedForVendor", "Assessment") }},
            {{ col_rename("ReAssessmentParentId", "Assessment") }},

            {{ col_rename("ReAssessmentRootId", "Assessment") }},
            {{ col_rename("ReAssessmentVersion", "Assessment") }},
            {{ col_rename("AuthorityId", "Assessment") }},
            {{ col_rename("PublishedById", "Assessment") }},

            {{ col_rename("ArchivedDate", "Assessment") }},
            {{ col_rename("ClosedDate", "Assessment") }},
            {{ col_rename("ClosedReason", "Assessment") }},
            {{ col_rename("ResponseCompletedDate", "Assessment") }},

            {{ col_rename("ResponseStartedDate", "Assessment") }},
            {{ col_rename("ReviewedDate", "Assessment") }},
            {{ col_rename("AssessmentVersion", "Assessment") }},
            {{ col_rename("ParentAssessmentId", "Assessment") }},

            {{ col_rename("RootAssessmentId", "Assessment") }},
            {{ col_rename("IsDeprecatedAssessmentVersion", "Assessment") }},
            {{ col_rename("CreatedFromAssessmentId", "Assessment") }},
            {{ col_rename("WorkFlowId", "Assessment") }},
            {{ col_rename("WorkFlow", "Assessment") }},
            {{ col_rename("TenantVendorId", "Assessment") }},
            {{ col_rename("IsCurrent", "Assessment") }},
            {{ col_rename("CreationTime", "Assessment") }},
            {{ col_rename("UpdateTime", "Assessment") }}
        from assessment a
        left join template tpl on a.CreatedFromTemplateId = tpl.Template_Id

    )
select *
from ass

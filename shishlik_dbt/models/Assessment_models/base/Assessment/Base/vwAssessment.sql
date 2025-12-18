{{ config(materialized="view") }}
{# 
DOC START
  - name: vwAssessment
    description: |
        One row for an assessment and its template.
        Assessment has IsTemplate = 0
        Template has IsTemplate = 1
    columns:
      - name: Assessment_Name_Responding_Team
        description: |
            Name of the Assessment with Responding Team (from TenantVendor Table) in brackets.
      - name: Assessment_AssessmentVersionName
        description: |
            Name of the Assessment Version in brackets if it is a deperecated version and "Active Version" if it is the current version.
      - name: Assessment_CreatedByTemplateId
        description: |
            FK to the template that created this assessment.  
            NULL value is filled with -1 to link enable full join of Assessments to Templates.

DOC END
#}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            case
                when IsTemplate = 1 and IsArchived = 1
                then 'No Template'  -- Template Name 
                else cast(a. [Name] as varchar(200))
            end
            [Name],
            cast(a. [Name] + ' (' + tv.TenantVendor_Name + ')' as varchar(200)) Name_Responding_Team,
            [Status],
            case
                when [Status] = 1
                then 'Draft'
                when [Status] = 2
                then 'Approved'
                when [Status] = 3
                then 'Published'
                when [Status] = 4
                then 'Completed'
                when [Status] = 5
                then 'Closed'
                when [Status] = 6
                then 'Reviewed'
                when [Status] = 7
                then 'In Progress'
                when [Status] = 8
                then 'Cancelled'
                else 'Undefined'
            end as [StatusCode],
            cast([Objective] as nvarchar(4000)) Objective,
            cast([Description] as nvarchar(4000)) Description,
            cast([Tags] as nvarchar(4000)) Tags,
            [DueDate],
            [IsTemplate],
            [TenantVendorId],
            [EngagementId],
            [PolicyId],
            cast([ConfirmationCode] as nvarchar(4000)) ConfirmationCode,
            [PublishedDate],
            cast([EndPage] as nvarchar(4000)) EndPage,
            cast([Introduction] as nvarchar(4000)) Introduction,
            [TemplateType],
            [TypeId],
            [QuestionType],
            case
                when [QuestionType] = 0
                then 'Preferred Answer'
                when [QuestionType] = 1
                then 'Weighted Score'
                when [QuestionType] = 2
                then 'Risk Rated'
                else 'Undefined'
            end QuestionTypeCode,
            [RoundResultToNearest],
            [ACROSSDomain],
            [WithinDomain],
            [ParentTemplateId],
            [RootTemplateId],
            [TemplateVersion],
            Name+' - v'+cast(TemplateVersion as varchar) [TemplateVersionName],
            COALESCE([CreatedFromTemplateId], -1) CreatedFromTemplateId,
            [ParentMarketplaceTemplateId],
            [BulkSendAssessmentLogId],
            [HasPeriod],
            [Period],
            [PeriodicAssessmentId],
            [PeriodStartDate],
            [IsArchived],
            cast([ImageUrl] as nvarchar(4000)) ImageUrl,
            [IsArchivedForVendor],
            [ReAssessmentParentId],
            [ReAssessmentRootId],
            [ReAssessmentVersion],
            [AuthorityId],
            [PublishedById],
            [ArchivedDate],
            [ClosedDate],
            cast([ClosedReason] as nvarchar(4000)) ClosedReason,
            [ResponseCompletedDate],
            [ResponseStartedDate],
            [ReviewedDate],
            [AssessmentVersion],
            case
                when AssessmentVersion is null
                then null
                when IsDeprecatedAssessmentVersion = 0
                then 'Active Version'
                else (cast(a. [Name] as varchar(100)) + ' (v' + cast(a. [AssessmentVersion] as varchar(8)) + ')')
            end as AssessmentVersionName,
            [ParentAssessmentId],
            [RootAssessmentId],
            [IsDeprecatedAssessmentVersion],
            [CreatedFromAssessmentId],
            [WorkFlowId],
            case
                when [WorkFlowId] = 1 then 'Requirement' when [WorkFlowId] = 0 then 'Question' else 'Undefined'
            end Workflow,
            case
                when
                    lead([CreationTime]) over (
                        partition by coalesce([RootAssessmentId], [Id]) order by [TemplateVersion]
                    )
                    is null
                then 1
                else 0
            end IsCurrent,
            CAST(GREATEST(LastModificationTime, CreationTime, tv.TenantVendor_UpdateTime) AS datetime2) UpdateTime
        from {{ source("assessment_models", "Assessment") }} a
        left join
            {{ ref("vwTenantVendor") }} tv on a.TenantVendorId = tv.TenantVendor_Id
            -- Note We need to determine the line item detail of this table.
            {{ system_remove_IsDeleted() }}
    ),
    [unknown] as ( -- Unknown template to join Assessment to Template.
        select
            -1 [Assessment_ID],
            NULL [Assessment_IsDeleted],
            NULL [Assessment_CreationTime],
            t. [Id] [Assessment_TenantId],
            'No Template' [Assessment_Name],
            NULL [Assessment_Name_Responding_Team],
            -- NULL [Assessment_Name_Responding_Team_Abp],
            NULL [Assessment_Status],
            NULL [Assessment_StatusCode],
            NULL [Assessment_Objective],
            NULL [Assessment_Description],
            NULL [Assessment_Tags],
            NULL [Assessment_DueDate],
            NULL AssessmentOverdueFlag,
            CONVERT(bit, 'TRUE') [Assessment_IsTemplate],
            NULL [Assessment_TenantVendorId],
            NULL [Assessment_EngagementId],
            NULL [Assessment_PolicyId],
            NULL [Assessment_ConfirmationCode],
            NULL [Assessment_PublishedDate],
            NULL [Assessment_EndPage],
            NULL [Assessment_Introduction],
            NULL [Assessment_TemplateType],
            NULL [Assessment_TypeId],
            NULL [Assessment_QuestionType],
            NULL [Assessment_QuestionTypeCode],
            NULL [Assessment_RoundResultToNearest],
            NULL [Assessment_ACROSSDomain],
            NULL [Assessment_WithinDomain],
            NULL [Assessment_ParentTemplateId],
            NULL [Assessment_RootTemplateId],
            NULL [Assessment_TemplateVersion],
            NULL [Assessment_TemplateVersionName],
            NULL [Assessment_CreatedFromTemplateId],
            NULL [Assessment_ParentMarketplaceTemplateId],
            NULL [Assessment_BulkSendAssessmentLogId],
            NULL [Assessment_HasPeriod],
            NULL [Assessment_Period],
            NULL [Assessment_PeriodicAssessmentId],
            NULL [Assessment_PeriodStartDate],
            NULL [Assessment_IsArchived],
            NULL [Assessment_ImageUrl],
            NULL [Assessment_IsArchivedForVendor],
            NULL [Assessment_ReAssessmentParentId],
            NULL [Assessment_ReAssessmentRootId],
            NULL [Assessment_ReAssessmentVersion],
            NULL [Assessment_AuthorityId],
            NULL [Assessment_PublishedById],
            NULL [Assessment_ArchivedDate],
            NULL [Assessment_ClosedDate],
            NULL [Assessment_ClosedReason],
            NULL [Assessment_ResponseCompletedDate],
            NULL [Assessment_ResponseStartedDate],
            NULL [Assessment_ReviewedDate],
            NULL [Assessment_AssessmentVersion],
            NULL [Assessment_AssessmentVersionName],
            NULL [Assessment_ParentAssessmentId],
            NULL [Assessment_RootAssessmentId],
            NULL [Assessment_IsDeprecatedAssessmentVersion],
            NULL [Assessment_CreatedFromAssessmentId],
            1 [Assessment_WorkFlowId],
            NULL [Assessment_WorkFlow],
            1 [Assessment_IsCurrent],
            cast(CreationTime as datetime2) Assessment_UpdateTime
        from {{ source("assessment_models", "AbpTenants") }} t
    ),
    ass as (
        select
            {{ col_rename("ID", "Assessment") }},
            {{ col_rename("IsDeleted", "Assessment") }},
            {{ col_rename("CreationTime", "Assessment") }},
            {{ col_rename("TenantId", "Assessment") }},
            {{ col_rename("Name", "Assessment") }},
            {{ col_rename("Name_Responding_Team", "Assessment") }},
            -- {{ col_rename('Name_Responding_Team_Abp','Assessment')}},
            {{ col_rename("Status", "Assessment") }},
            {{ col_rename("StatusCode", "Assessment") }},

            {{ col_rename("Objective", "Assessment") }},
            {{ col_rename("Description", "Assessment") }},
            {{ col_rename("Tags", "Assessment") }},
            {{ col_rename("DueDate", "Assessment") }},
            case
                when Status in (4, 5, 6)
                then 'No'
                when Status not in (4, 5, 6) and DueDate is null
                then 'No Due Date'
                -- when Assessment_Status in (4, 5, 6) and a.Assessment_DueDate is null then 'No DueDate'
                when Status not in (4, 5, 6) and DueDate < getdate()
                then 'yes'
                else 'No'
            end as AssessmentOverdueFlag,

            {{ col_rename("IsTemplate", "Assessment") }},
            {{ col_rename("TenantVendorId", "Assessment") }},
            {{ col_rename("EngagementId", "Assessment") }},
            {{ col_rename("PolicyId", "Assessment") }},

            {{ col_rename("ConfirmationCode", "Assessment") }},
            {{ col_rename("PublishedDate", "Assessment") }},
            {{ col_rename("EndPage", "Assessment") }},
            {{ col_rename("Introduction", "Assessment") }},

            {{ col_rename("TemplateType", "Assessment") }},
            {{ col_rename("TypeId", "Assessment") }},
            {{ col_rename("QuestionType", "Assessment") }},
            {{ col_rename("QuestionTypeCode", "Assessment") }},
            {{ col_rename("RoundResultToNearest", "Assessment") }},

            {{ col_rename("ACROSSDomain", "Assessment") }},
            {{ col_rename("WithinDomain", "Assessment") }},
            {{ col_rename("ParentTemplateId", "Assessment") }},
            {{ col_rename("RootTemplateId", "Assessment") }},

            {{ col_rename("TemplateVersion", "Assessment") }},
            {{ col_rename("TemplateVersionName", "Assessment") }},
            {{ col_rename("CreatedFromTemplateId", "Assessment") }},
            {{ col_rename("ParentMarketplaceTemplateId", "Assessment") }},
            {{ col_rename("BulkSendAssessmentLogId", "Assessment") }},

            {{ col_rename("HasPeriod", "Assessment") }},
            {{ col_rename("Period", "Assessment") }},
            {{ col_rename("PeriodicAssessmentId", "Assessment") }},
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
            {{ col_rename("AssessmentVersionName", "Assessment") }},
            {{ col_rename("ParentAssessmentId", "Assessment") }},

            {{ col_rename("RootAssessmentId", "Assessment") }},
            {{ col_rename("IsDeprecatedAssessmentVersion", "Assessment") }},
            {{ col_rename("CreatedFromAssessmentId", "Assessment") }},
            {{ col_rename("WorkFlowId", "Assessment") }},
            {{ col_rename("WorkFlow", "Assessment") }},
            {{ col_rename("IsCurrent", "Assessment") }},
            {{ col_rename("UpdateTime", "Assessment") }}
        from base
    )
select *
from ass

union all

select *
from [unknown]

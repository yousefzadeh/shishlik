{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast(a. [Name] as varchar(200))[Name],
            cast(a. [Name] + ' (' + tv.TenantVendor_Name + ')' as varchar(200)) Name_Responding_Team,
            -- ,a.[Name] + ' ('+ t.AbpTenants_Name +', '+ tv.TenantVendor_Name + ')' Name_Responding_Team_Abp
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
            coalesce(LastModificationTime, CreationTime) UpdateTime
        from {{ source("assessment_models", "Assessment") }} a
        left join
            {{ ref("vwTenantVendor") }} tv on a.TenantVendorId = tv.TenantVendor_Id
            -- join {{ ref('vwAbpTenants') }} t
            -- on t.AbpTenants_Id = a.TenantId
            -- Note We need to determine the line item detail of this table.
            {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "Template") }},
    {{ col_rename("TenantId", "Template") }},
    {{ col_rename("Name_Responding_Team", "Template") }},
    {{ col_rename("Name", "Template") }},
    {{ col_rename("Status", "Template") }},
    {{ col_rename("StatusCode", "Template") }},

    {{ col_rename("Objective", "Template") }},
    {{ col_rename("Description", "Template") }},
    {{ col_rename("Tags", "Template") }},
    {{ col_rename("DueDate", "Template") }},

    {{ col_rename("IsTemplate", "Template") }},
    {{ col_rename("TenantVendorId", "Template") }},
    {{ col_rename("EngagementId", "Template") }},
    {{ col_rename("PolicyId", "Template") }},

    {{ col_rename("ConfirmationCode", "Template") }},
    {{ col_rename("PublishedDate", "Template") }},
    {{ col_rename("EndPage", "Template") }},
    {{ col_rename("Introduction", "Template") }},

    {{ col_rename("TemplateType", "Template") }},
    {{ col_rename("TypeId", "Template") }},
    {{ col_rename("QuestionType", "Template") }},
    {{ col_rename("QuestionTypeCode", "Template") }},
    {{ col_rename("RoundResultToNearest", "Template") }},

    {{ col_rename("ACROSSDomain", "Template") }},
    {{ col_rename("WithinDomain", "Template") }},
    {{ col_rename("ParentTemplateId", "Template") }},
    {{ col_rename("RootTemplateId", "Template") }},

    {{ col_rename("TemplateVersion", "Template") }},
    {{ col_rename("CreatedFromTemplateId", "Template") }},
    {{ col_rename("ParentMarketplaceTemplateId", "Template") }},
    {{ col_rename("BulkSendAssessmentLogId", "Template") }},

    {{ col_rename("HasPeriod", "Template") }},
    {{ col_rename("Period", "Template") }},
    {{ col_rename("PeriodicAssessmentId", "Template") }},
    {{ col_rename("PeriodStartDate", "Template") }},

    {{ col_rename("IsArchived", "Template") }},
    {{ col_rename("ImageUrl", "Template") }},
    {{ col_rename("IsArchivedForVendor", "Template") }},
    {{ col_rename("ReAssessmentParentId", "Template") }},

    {{ col_rename("ReAssessmentRootId", "Template") }},
    {{ col_rename("ReAssessmentVersion", "Template") }},
    {{ col_rename("AuthorityId", "Template") }},
    {{ col_rename("PublishedById", "Template") }},

    {{ col_rename("ArchivedDate", "Template") }},
    {{ col_rename("ClosedDate", "Template") }},
    {{ col_rename("ClosedReason", "Template") }},
    {{ col_rename("ResponseCompletedDate", "Template") }},

    {{ col_rename("ResponseStartedDate", "Template") }},
    {{ col_rename("ReviewedDate", "Template") }},
    {{ col_rename("AssessmentVersion", "Template") }},
    {{ col_rename("ParentAssessmentId", "Template") }},

    {{ col_rename("RootAssessmentId", "Template") }},
    {{ col_rename("IsDeprecatedAssessmentVersion", "Template") }},
    {{ col_rename("CreatedFromAssessmentId", "Template") }},
    {{ col_rename("WorkFlowId", "Template") }},
    {{ col_rename("WorkFlow", "Template") }},
    {{ col_rename("IsCurrent", "Template") }},
    {{ col_rename("UpdateTime", "Template") }}
from base
where base.IsTemplate = 1 and base.IsArchived = 0 and base.Status in (3, 100)

{# 
DOC START
  - name: vwRBA_TemplateAssessment
    description: |
      One row for each Requirements Based Assessments (RBA) Template and Assessment.
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
with final as (
    select
    *
    from {{ ref("vwAllTemplateAssessment") }}
    where Assessment_WorkFlowId = 1 -- Requirements Based Assessments Only 
)
select 
Template_Id,
Template_Name,
Template_Status,
Template_StatusCode,
Template_PublishedDate,
Template_PublishedById,
Assessment_ID,
Assessment_TenantId,
Assessment_RespondingTeam,
Assessment_Name,
Assessment_Name_RespondingTeam,
Assessment_Status,
Assessment_StatusCode,
Assessment_Objective,
Assessment_Description,
Assessment_Tags,
Assessment_DueDate,
Assessment_OverdueFlag,
Assessment_EngagementId,
Assessment_PolicyId,
Assessment_PublishedDate,
Assessment_TypeId,
Assessment_QuestionType,
Assessment_QuestionTypeCode,
Assessment_RoundResultToNearest,
Assessment_ACROSSDomain,
Assessment_WithinDomain,
Assessment_ParentTemplateId,
Assessment_RootTemplateId,
Assessment_TemplateVersion,
Assessment_HasPeriod,
Assessment_Period,
Assessment_PeriodCode,
Assessment_PeriodStartDate,
Assessment_IsArchived,
Assessment_ImageUrl,
Assessment_IsArchivedForVendor,
Assessment_ReAssessmentParentId,
Assessment_ReAssessmentRootId,
Assessment_ReAssessmentVersion,
Assessment_AuthorityId,
Assessment_PublishedById,
Assessment_ArchivedDate,
Assessment_ClosedDate,
Assessment_ClosedReason,
Assessment_ResponseCompletedDate,
Assessment_ResponseStartedDate,
Assessment_ReviewedDate,
Assessment_AssessmentVersion,
Assessment_ParentAssessmentId,
Assessment_RootAssessmentId,
Assessment_IsDeprecatedAssessmentVersion,
Assessment_CreatedFromAssessmentId,
Assessment_WorkFlowId,
Assessment_WorkFlow,
Assessment_TenantVendorId,
Assessment_IsCurrent,
Assessment_CreationTime,
Assessment_UpdateTime
from final

{{ config(materialized="view") }}
-- Notes some assessments do not have an authority
with
    base as (
        select
            [Assessment_ID],
            [Assessment_TenantId],
            [Assessment_Name],
            [Assessment_Status],

            [Assessment_Objective],
            [Assessment_Description],
            [Assessment_Tags],
            [Assessment_DueDate],

            [Assessment_IsTemplate],
            [Assessment_TenantVendorId],
            [Assessment_EngagementId],
            [Assessment_PolicyId],

            [Assessment_ConfirmationCode],
            [Assessment_PublishedDate],
            [Assessment_EndPage],
            [Assessment_Introduction],

            [Assessment_TemplateType],
            [Assessment_TypeId],
            [Assessment_QuestionType],
            [Assessment_RoundResultToNearest],

            [Assessment_ACROSSDomain],
            [Assessment_WithinDomain],
            [Assessment_ParentTemplateId],
            [Assessment_RootTemplateId],

            [Assessment_TemplateVersion],
            [Assessment_CreatedFromTemplateId],
            [Assessment_ParentMarketplaceTemplateId],
            [Assessment_BulkSendAssessmentLogId],

            [Assessment_HasPeriod],
            [Assessment_Period],
            [Assessment_PeriodicAssessmentId],
            [Assessment_PeriodStartDate],

            [Assessment_IsArchived],
            [Assessment_ImageUrl],
            [Assessment_IsArchivedForVendor],
            [Assessment_ReAssessmentParentId],

            [Assessment_ReAssessmentRootId],
            [Assessment_ReAssessmentVersion],
            [Assessment_AuthorityId],
            [Assessment_PublishedById],

            [Assessment_ArchivedDate],
            [Assessment_ClosedDate],
            [Assessment_ClosedReason],
            [Assessment_ResponseCompletedDate],

            [Assessment_ResponseStartedDate],
            [Assessment_ReviewedDate],
            [Assessment_AssessmentVersion],
            [Assessment_ParentAssessmentId],

            [Assessment_RootAssessmentId],
            [Assessment_IsDeprecatedAssessmentVersion],
            [Assessment_CreatedFromAssessmentId],
            [Assessment_WorkFlowId]
        from {{ ref("vwAssessment") }}
        -- Note We need to determine the line item detail of this table.
        where Assessment_WorkFlowId = 1  -- Filter for Requirements based assessments
    )

select
    [Assessment_ID],
    [Assessment_TenantId],
    [Assessment_Name],
    [Assessment_Status],

    [Assessment_Objective],
    [Assessment_Description],
    [Assessment_Tags],

    [Assessment_DueDate],
    [Assessment_IsTemplate],
    [Assessment_TenantVendorId],
    [Assessment_EngagementId],

    [Assessment_PolicyId],
    [Assessment_ConfirmationCode],
    [Assessment_PublishedDate],
    [Assessment_EndPage],

    [Assessment_Introduction],
    [Assessment_TemplateType],
    [Assessment_TypeId],
    [Assessment_QuestionType],

    [Assessment_RoundResultToNearest],
    [Assessment_ACROSSDomain],
    [Assessment_WithinDomain],
    [Assessment_ParentTemplateId],

    [Assessment_RootTemplateId],
    [Assessment_TemplateVersion],
    [Assessment_CreatedFromTemplateId],
    [Assessment_ParentMarketplaceTemplateId],

    [Assessment_BulkSendAssessmentLogId],
    [Assessment_HasPeriod],
    [Assessment_Period],
    [Assessment_PeriodicAssessmentId],

    [Assessment_PeriodStartDate],
    [Assessment_IsArchived],
    [Assessment_ImageUrl],
    [Assessment_IsArchivedForVendor],

    [Assessment_ReAssessmentParentId],
    [Assessment_ReAssessmentRootId],
    [Assessment_ReAssessmentVersion],
    [Assessment_AuthorityId],

    [Assessment_PublishedById],
    [Assessment_ArchivedDate],
    [Assessment_ClosedDate],
    [Assessment_ClosedReason],

    [Assessment_ResponseCompletedDate],
    [Assessment_ResponseStartedDate],
    [Assessment_ReviewedDate],
    [Assessment_AssessmentVersion],

    [Assessment_ParentAssessmentId],
    [Assessment_RootAssessmentId],
    [Assessment_IsDeprecatedAssessmentVersion],
    [Assessment_CreatedFromAssessmentId],

    [Assessment_WorkFlowId]
from base

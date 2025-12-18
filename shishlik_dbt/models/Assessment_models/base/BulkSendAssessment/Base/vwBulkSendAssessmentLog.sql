{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            cast([CustomMessage] as nvarchar(4000)) CustomMessage,
            [Status],
            cast([Error] as nvarchar(4000)) Error,
            [TenantId],
            cast([BackgroundJobId] as nvarchar(4000)) BackgroundJobId,
            [AssessmentTemplateId],
            cast([ErrorStackTrace] as nvarchar(4000)) ErrorStackTrace,
            [DueDate],
            cast([EngagementName] as nvarchar(4000)) EngagementName
        from {{ source("assessment_models", "BulkSendAssessmentLog") }}
    )

select
    {{ col_rename("Id", "BulkSendAssessmentLog") }},
    {{ col_rename("CreationTime", "BulkSendAssessmentLog") }},
    {{ col_rename("CreatorUserId", "BulkSendAssessmentLog") }},
    {{ col_rename("CustomMessage", "BulkSendAssessmentLog") }},

    {{ col_rename("Status", "BulkSendAssessmentLog") }},
    {{ col_rename("Error", "BulkSendAssessmentLog") }},
    {{ col_rename("TenantId", "BulkSendAssessmentLog") }},
    {{ col_rename("BackgroundJobId", "BulkSendAssessmentLog") }},

    {{ col_rename("AssessmentTemplateId", "BulkSendAssessmentLog") }},
    {{ col_rename("ErrorStackTrace", "BulkSendAssessmentLog") }},
    {{ col_rename("DueDate", "BulkSendAssessmentLog") }},
    {{ col_rename("EngagementName", "BulkSendAssessmentLog") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [HasError],
            cast([ErrorMessage] as nvarchar(4000)) ErrorMessage,
            [IsEmailSent],
            [IsChainPointProofCreated],
            [TenantId],
            [BulkSendAssessmentLogId],
            [TenantVendorId]
        from {{ source("assessment_models", "BulkSendAssessmentRecord") }}
    )

select
    {{ col_rename("Id", "BulkSendAssessmentRecord") }},
    {{ col_rename("CreationTime", "BulkSendAssessmentRecord") }},
    {{ col_rename("CreatorUserId", "BulkSendAssessmentRecord") }},
    {{ col_rename("HasError", "BulkSendAssessmentRecord") }},

    {{ col_rename("ErrorMessage", "BulkSendAssessmentRecord") }},
    {{ col_rename("IsEmailSent", "BulkSendAssessmentRecord") }},
    {{ col_rename("IsChainPointProofCreated", "BulkSendAssessmentRecord") }},
    {{ col_rename("TenantId", "BulkSendAssessmentRecord") }},

    {{ col_rename("BulkSendAssessmentLogId", "BulkSendAssessmentRecord") }},
    {{ col_rename("TenantVendorId", "BulkSendAssessmentRecord") }}
from base

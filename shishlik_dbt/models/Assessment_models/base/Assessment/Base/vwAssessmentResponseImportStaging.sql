{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [TenantId],
            cast([ReferenceId] as nvarchar(4000)) ReferenceId,
            cast([Title] as nvarchar(4000)) Title,
            cast([CustomData] as nvarchar(4000)) CustomData,
            [HasError],
            cast([ErrorMessage] as nvarchar(4000)) ErrorMessage,
            [IsExists],
            [IsDuplicate],
            [IsImported],
            [ImportFileLogId]
        from {{ source("assessment_models", "AssessmentResponseImportStaging") }}
    )

select
    {{ col_rename("ID", "AssessmentResponseImportStaging") }},
    {{ col_rename("TenantId", "AssessmentResponseImportStaging") }},
    {{ col_rename("ReferenceId", "AssessmentResponseImportStaging") }},
    {{ col_rename("Title", "AssessmentResponseImportStaging") }},

    {{ col_rename("CustomData", "AssessmentResponseImportStaging") }},
    {{ col_rename("HasError", "AssessmentResponseImportStaging") }},
    {{ col_rename("ErrorMessage", "AssessmentResponseImportStaging") }},
    {{ col_rename("IsExists", "AssessmentResponseImportStaging") }},

    {{ col_rename("IsDuplicate", "AssessmentResponseImportStaging") }},
    {{ col_rename("IsImported", "AssessmentResponseImportStaging") }},
    {{ col_rename("ImportFileLogId", "AssessmentResponseImportStaging") }}
from base

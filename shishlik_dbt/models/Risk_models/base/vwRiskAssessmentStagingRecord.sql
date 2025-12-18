{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [TenantId],
            cast([RiskName] as nvarchar(4000)) RiskName,
            cast([RiskAssessmentTitle] as nvarchar(4000)) RiskAssessmentTitle,
            cast([Likelihood] as nvarchar(4000)) Likelihood,
            cast([Impact] as nvarchar(4000)) Impact,
            [HasError],
            cast([ErrorMessage] as nvarchar(4000)) ErrorMessage,
            [IsExists],
            [IsDuplicate],
            [IsImported],
            [ImportFileLogId],
            cast([CustomData] as nvarchar(4000)) CustomData,
            cast([RiskAssessmentLabel] as nvarchar(4000)) RiskAssessmentLabel,
            cast([RiskAssessmentDate] as nvarchar(4000)) RiskAssessmentDate
        from {{ source("risk_models", "RiskAssessmentStagingRecord") }}

    )

select
    {{ col_rename("Id", "RiskAssessmentStagingRecord") }},
    {{ col_rename("CreationTime", "RiskAssessmentStagingRecord") }},
    {{ col_rename("CreatorUserId", "RiskAssessmentStagingRecord") }},
    {{ col_rename("TenantId", "RiskAssessmentStagingRecord") }},

    {{ col_rename("RiskName", "RiskAssessmentStagingRecord") }},
    {{ col_rename("RiskAssessmentTitle", "RiskAssessmentStagingRecord") }},
    {{ col_rename("Likelihood", "RiskAssessmentStagingRecord") }},
    {{ col_rename("Impact", "RiskAssessmentStagingRecord") }},

    {{ col_rename("HasError", "RiskAssessmentStagingRecord") }},
    {{ col_rename("ErrorMessage", "RiskAssessmentStagingRecord") }},
    {{ col_rename("IsExists", "RiskAssessmentStagingRecord") }},
    {{ col_rename("IsDuplicate", "RiskAssessmentStagingRecord") }},

    {{ col_rename("IsImported", "RiskAssessmentStagingRecord") }},
    {{ col_rename("ImportFileLogId", "RiskAssessmentStagingRecord") }},
    {{ col_rename("CustomData", "RiskAssessmentStagingRecord") }},
    {{ col_rename("RiskAssessmentLabel", "RiskAssessmentStagingRecord") }},
    {{ col_rename("RiskAssessmentDate", "RiskAssessmentStagingRecord") }}
from base

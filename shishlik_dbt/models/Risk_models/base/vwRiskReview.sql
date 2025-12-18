{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Name] as nvarchar(4000))[Name],
            cast([Description] as nvarchar(4000)) Description,
            [DueDate],
            [Status],
            [IsArchived],
            IsPreviousStatusInProgress,
            StartDate,
            MigratedFromRiskReviewId
        from {{ source("risk_models", "RiskReviewNew") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskReview") }},
    {{ col_rename("CreationTime", "RiskReview") }},
    {{ col_rename("CreatorUserId", "RiskReview") }},
    {{ col_rename("LastModificationTime", "RiskReview") }},

    {{ col_rename("LastModifierUserId", "RiskReview") }},
    {{ col_rename("IsDeleted", "RiskReview") }},
    {{ col_rename("DeleterUserId", "RiskReview") }},
    {{ col_rename("DeletionTime", "RiskReview") }},

    {{ col_rename("TenantId", "RiskReview") }},
    {{ col_rename("Name", "RiskReview") }},
    {{ col_rename("Description", "RiskReview") }},

    {{ col_rename("DueDate", "RiskReview") }},
    {{ col_rename("Status", "RiskReview") }},
    {{ col_rename("IsArchived", "RiskReview") }},
    {{ col_rename("IsPreviousStatusInProgress", "RiskReview") }},
    {{ col_rename("StartDate", "RiskReview") }},
    {{ col_rename("MigratedFromRiskReviewId", "RiskReview") }}
from base

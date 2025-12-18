{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [RiskReviewId],
            [RiskId],
            [IsReviewed],
            [ReviewedById],
            [ReviewedDate],
            [MigratedReviewedByIds]
        from {{ source("reviewrisks_models", "ReviewRisksNew") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ReviewRisks") }},
    {{ col_rename("TenantId", "ReviewRisks") }},
    {{ col_rename("RiskReviewId", "ReviewRisks") }},
    {{ col_rename("RiskId", "ReviewRisks") }},

    {{ col_rename("IsReviewed", "ReviewRisks") }},
    {{ col_rename("ReviewedById", "ReviewRisks") }},
    {{ col_rename("ReviewedDate", "ReviewRisks") }},
    {{ col_rename("MigratedReviewedByIds", "ReviewRisks") }}
from base

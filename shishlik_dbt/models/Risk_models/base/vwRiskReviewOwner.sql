{{ config(materialized="view") }}

with
    base as (
        select
            Id,
            [TenantId],
            RiskReviewId,
            UserId,
            CreationTime,
            LastModificationTime
        from {{ source("risk_models", "RiskReviewOwnerNew") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskReviewOwner") }},
    {{ col_rename("CreationTime", "RiskReviewOwner") }},
    {{ col_rename("LastModificationTime", "RiskReviewOwner") }},

    {{ col_rename("TenantId", "RiskReviewOwner") }},
    {{ col_rename("RiskReviewId", "RiskReviewOwner") }},
    {{ col_rename("UserId", "RiskReviewOwner") }}
from base

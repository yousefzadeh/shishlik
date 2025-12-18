{{ config(materialized="view") }}

with
    base as (
        select [Id], [CreationTime], [CreatorUserId], [TenantId], [RiskReviewId], [OwnerId], [UserId], [UserEvent]
        from {{ source("risk_models", "RiskReviewOwnerEmailsLog") }}

    )

select
    {{ col_rename("Id", "RiskReviewOwnerEmailsLog") }},
    {{ col_rename("CreationTime", "RiskReviewOwnerEmailsLog") }},
    {{ col_rename("CreatorUserId", "RiskReviewOwnerEmailsLog") }},
    {{ col_rename("TenantId", "RiskReviewOwnerEmailsLog") }},

    {{ col_rename("RiskReviewId", "RiskReviewOwnerEmailsLog") }},
    {{ col_rename("OwnerId", "RiskReviewOwnerEmailsLog") }},
    {{ col_rename("UserId", "RiskReviewOwnerEmailsLog") }},
    {{ col_rename("UserEvent", "RiskReviewOwnerEmailsLog") }}
from base

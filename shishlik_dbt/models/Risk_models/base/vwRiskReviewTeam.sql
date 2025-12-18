{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [UserId], [RiskReviewId], [RoleId], [Status]
        from {{ source("risk_models", "RiskReviewTeam") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskReviewTeam") }},
    {{ col_rename("CreationTime", "RiskReviewTeam") }},
    {{ col_rename("CreatorUserId", "RiskReviewTeam") }},
    {{ col_rename("LastModificationTime", "RiskReviewTeam") }},

    {{ col_rename("LastModifierUserId", "RiskReviewTeam") }},
    {{ col_rename("IsDeleted", "RiskReviewTeam") }},
    {{ col_rename("DeleterUserId", "RiskReviewTeam") }},
    {{ col_rename("DeletionTime", "RiskReviewTeam") }},

    {{ col_rename("TenantId", "RiskReviewTeam") }},
    {{ col_rename("UserId", "RiskReviewTeam") }},
    {{ col_rename("RiskReviewId", "RiskReviewTeam") }},
    {{ col_rename("RoleId", "RiskReviewTeam") }},
    {{ col_rename("Status", "RiskReviewTeam") }}
from base

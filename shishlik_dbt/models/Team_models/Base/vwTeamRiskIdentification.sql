{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [LastModificationTime],
            [LastModifierUserId],
            [UserId],
            [AbstractRiskId],
            [RiskReviewId],
            [IdentificationStatus],
            [TenantId],
            [IsSyncedWithGraphDb]
        from {{ source("Team_models", "TeamRiskIdentification") }}
    )

select
    {{ col_rename("Id", "TeamRiskIdentification") }},
    {{ col_rename("UserId", "TeamRiskIdentification") }},
    {{ col_rename("AbstractRiskId", "TeamRiskIdentification") }},
    {{ col_rename("RiskReviewId", "TeamRiskIdentification") }},

    {{ col_rename("IdentificationStatus", "TeamRiskIdentification") }},
    {{ col_rename("TenantId", "TeamRiskIdentification") }},
    {{ col_rename("IsSyncedWithGraphDb", "TeamRiskIdentification") }}
from base

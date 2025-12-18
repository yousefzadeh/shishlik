{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [LastModificationTime],
            [LastModifierUserId],
            [XAxisThirdPartyAttributeId],
            [YAxisThirdPartyAttributeId],
            [RiskRatingId],
            [UserId],
            [RiskId],
            [TenantId],
            [IsSyncedWithGraphDb]
        from {{ source("Team_models", "TeamRiskRating") }}
    )

select
    {{ col_rename("Id", "TeamRiskRating") }},
    {{ col_rename("XAxisThirdPartyAttributeId", "TeamRiskRating") }},
    {{ col_rename("YAxisThirdPartyAttributeId", "TeamRiskRating") }},
    {{ col_rename("RiskRatingId", "TeamRiskRating") }},
    {{ col_rename("CreationTime", "TeamRiskRating") }},
    {{ col_rename("LastModificationTime", "TeamRiskRating") }},

    {{ col_rename("UserId", "TeamRiskRating") }},
    {{ col_rename("RiskId", "TeamRiskRating") }},
    {{ col_rename("TenantId", "TeamRiskRating") }},
    {{ col_rename("IsSyncedWithGraphDb", "TeamRiskRating") }}
from base

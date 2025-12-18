{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [TenantId],
            cast([Name] as nvarchar(4000))[Name],
            cast([EmailAddress] as nvarchar(4000)) EmailAddress,
            [RoleId],
            cast([VerificationCode] as nvarchar(4000)) VerificationCode,
            [RiskReviewId],
            [InvitedUserAdded]
        from {{ source("risk_models", "RiskReviewUserInvite") }}

    )

select
    {{ col_rename("Id", "RiskReviewUserInvite") }},
    {{ col_rename("CreationTime", "RiskReviewUserInvite") }},
    {{ col_rename("CreatorUserId", "RiskReviewUserInvite") }},
    {{ col_rename("TenantId", "RiskReviewUserInvite") }},

    {{ col_rename("Name", "RiskReviewUserInvite") }},
    {{ col_rename("EmailAddress", "RiskReviewUserInvite") }},
    {{ col_rename("RoleId", "RiskReviewUserInvite") }},
    {{ col_rename("VerificationCode", "RiskReviewUserInvite") }},

    {{ col_rename("RiskReviewId", "RiskReviewUserInvite") }},
    {{ col_rename("InvitedUserAdded", "RiskReviewUserInvite") }}
from base

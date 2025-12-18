{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([EmailAddress] as nvarchar(4000)) EmailAddress,
            [RiskReviewId],
            [InviteSent],
            cast([ErrorMessage] as nvarchar(4000)) ErrorMessage,
            [InvitedClientAdded],
            [InviteSentOn]
        from {{ source("risk_models", "RiskReviewClientInvite") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskReviewClientInvite") }},
    {{ col_rename("CreationTime", "RiskReviewClientInvite") }},
    {{ col_rename("CreatorUserId", "RiskReviewClientInvite") }},
    {{ col_rename("LastModificationTime", "RiskReviewClientInvite") }},

    {{ col_rename("LastModifierUserId", "RiskReviewClientInvite") }},
    {{ col_rename("IsDeleted", "RiskReviewClientInvite") }},
    {{ col_rename("DeleterUserId", "RiskReviewClientInvite") }},
    {{ col_rename("DeletionTime", "RiskReviewClientInvite") }},

    {{ col_rename("TenantId", "RiskReviewClientInvite") }},
    {{ col_rename("EmailAddress", "RiskReviewClientInvite") }},
    {{ col_rename("RiskReviewId", "RiskReviewClientInvite") }},
    {{ col_rename("InviteSent", "RiskReviewClientInvite") }},

    {{ col_rename("ErrorMessage", "RiskReviewClientInvite") }},
    {{ col_rename("InvitedClientAdded", "RiskReviewClientInvite") }},
    {{ col_rename("InviteSentOn", "RiskReviewClientInvite") }}
from base

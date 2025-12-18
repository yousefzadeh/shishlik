{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([EmailAddress] as nvarchar(4000)) EmailAddress,
            [TenantId],
            [ServiceProviderId],
            cast([ConfirmationCode] as nvarchar(4000)) ConfirmationCode,
            [ExpireTimeUtc],
            [IsVerified],
            [HasAcceptedInvite],
            [InviteAcceptedOn],
            [VerifiedOn],
            [UserIdInTenant],
            [IsAutoAcceptedInvite]
        from {{ source("assessment_models", "AdvisorInvite") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AdvisorInvite") }},
    {{ col_rename("EmailAddress", "AdvisorInvite") }},
    {{ col_rename("TenantId", "AdvisorInvite") }},
    {{ col_rename("ServiceProviderId", "AdvisorInvite") }},

    {{ col_rename("ConfirmationCode", "AdvisorInvite") }},
    {{ col_rename("ExpireTimeUtc", "AdvisorInvite") }},
    {{ col_rename("IsVerified", "AdvisorInvite") }},
    {{ col_rename("HasAcceptedInvite", "AdvisorInvite") }},

    {{ col_rename("InviteAcceptedOn", "AdvisorInvite") }},
    {{ col_rename("VerifiedOn", "AdvisorInvite") }},
    {{ col_rename("UserIdInTenant", "AdvisorInvite") }},
    {{ col_rename("IsAutoAcceptedInvite", "AdvisorInvite") }}
from base

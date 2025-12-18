{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([EmailAddress] as nvarchar(4000)) EmailAddress,
            cast([ConfirmationCode] as nvarchar(4000)) ConfirmationCode,
            [ExpireTimeUtc],
            [IsVerified],
            [VerifiedOn],
            [HasAcceptedInvite],
            [InviteAcceptedOn]
        from {{ source("tenant_models", "TenantUserInvite") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "TenantUserInvite") }},
    {{ col_rename("TenantId", "TenantUserInvite") }},
    {{ col_rename("EmailAddress", "TenantUserInvite") }},
    {{ col_rename("ConfirmationCode", "TenantUserInvite") }},

    {{ col_rename("ExpireTimeUtc", "TenantUserInvite") }},
    {{ col_rename("IsVerified", "TenantUserInvite") }},
    {{ col_rename("VerifiedOn", "TenantUserInvite") }},
    {{ col_rename("HasAcceptedInvite", "TenantUserInvite") }},

    {{ col_rename("InviteAcceptedOn", "TenantUserInvite") }}
from base

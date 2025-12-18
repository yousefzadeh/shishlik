{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [LastModificationTime],
            [LastModifierUserId],
            [VendorId],
            cast([AdminEmailAddress] as nvarchar(4000)) AdminEmailAddress,
            cast([VerificationCode] as nvarchar(4000)) VerificationCode,
            [IsVendorAdminVerified],
            [IsAutoVerified],
            [IsVendorAdminVerificationRequired]
        from {{ source("VendorAdminVerification_models", "VendorAdminVerification") }}
    )

select
    {{ col_rename("Id", "VendorAdminVerification") }},
    {{ col_rename("VendorId", "VendorAdminVerification") }},
    {{ col_rename("AdminEmailAddress", "VendorAdminVerification") }},
    {{ col_rename("VerificationCode", "VendorAdminVerification") }},

    {{ col_rename("IsVendorAdminVerified", "VendorAdminVerification") }},
    {{ col_rename("IsAutoVerified", "VendorAdminVerification") }},
    {{ col_rename("IsVendorAdminVerificationRequired", "VendorAdminVerification") }}
from base

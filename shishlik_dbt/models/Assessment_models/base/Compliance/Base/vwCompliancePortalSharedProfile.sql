{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [CompliancePortalProfileId],
            cast([FirstName] as nvarchar(4000)) FirstName,
            cast([LastName] as nvarchar(4000)) LastName,
            cast([EmailAddress] as nvarchar(4000)) EmailAddress,
            cast([Company] as nvarchar(4000)) Company,
            cast([Comments] as nvarchar(4000)) Comments,
            [UserId],
            cast([VerificationCode] as nvarchar(4000)) VerificationCode,
            [AccessRevoked]
        from {{ source("assessment_models", "CompliancePortalSharedProfile") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "CompliancePortalSharedProfile") }},
    {{ col_rename("TenantId", "CompliancePortalSharedProfile") }},
    {{ col_rename("CompliancePortalProfileId", "CompliancePortalSharedProfile") }},
    {{ col_rename("FirstName", "CompliancePortalSharedProfile") }},

    {{ col_rename("LastName", "CompliancePortalSharedProfile") }},
    {{ col_rename("EmailAddress", "CompliancePortalSharedProfile") }},
    {{ col_rename("Company", "CompliancePortalSharedProfile") }},
    {{ col_rename("Comments", "CompliancePortalSharedProfile") }},

    {{ col_rename("UserId", "CompliancePortalSharedProfile") }},
    {{ col_rename("VerificationCode", "CompliancePortalSharedProfile") }},
    {{ col_rename("AccessRevoked", "CompliancePortalSharedProfile") }}
from base

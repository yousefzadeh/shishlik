{{ config(materialized="view") }}
with
    base as (
        select Id, TenantId, CreationTime, LastModificationTime, FormId, UserId, OrganizationUnitId, OwnerType,
        case when OwnerType = 1 then 'Assessment Owners'
        when OwnerType = 2 then 'ThirdParty Owners' end OwnerTypeCode
        from {{ source("issue_models", "ThirdPartyOnboardingFormOwner") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ThirdPartyOnboardingFormOwner") }},
    {{ col_rename("CreationTime", "ThirdPartyOnboardingFormOwner") }},
    {{ col_rename("LastModificationTime", "ThirdPartyOnboardingFormOwner") }},
    {{ col_rename("TenantId", "ThirdPartyOnboardingFormOwner") }},
    {{ col_rename("FormId", "ThirdPartyOnboardingFormOwner") }},
    {{ col_rename("UserId", "ThirdPartyOnboardingFormOwner") }},

    {{ col_rename("OrganizationUnitId", "ThirdPartyOnboardingFormOwner") }},
    {{ col_rename("OwnerType", "ThirdPartyOnboardingFormOwner") }},
    {{ col_rename("OwnerTypeCode", "ThirdPartyOnboardingFormOwner") }}
from base

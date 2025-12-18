{{ config(materialized="view") }}
with
    base as (
        select Id, TenantId, Name, Url, IsArchived, SubmissionCount, Status, CreationTime, CreatorUserId,
        LastModificationTime, LastModifierUserId, CreatedFromFormId
        from {{ source("issue_models", "ThirdPartyOnboardingForm") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ThirdPartyOnboardingForm") }},
    {{ col_rename("CreationTime", "ThirdPartyOnboardingForm") }},
    {{ col_rename("CreatorUserId", "ThirdPartyOnboardingForm") }},
    {{ col_rename("LastModificationTime", "ThirdPartyOnboardingForm") }},

    {{ col_rename("LastModifierUserId", "ThirdPartyOnboardingForm") }},
    {{ col_rename("TenantId", "ThirdPartyOnboardingForm") }},
    {{ col_rename("Name", "ThirdPartyOnboardingForm") }},
    {{ col_rename("Url", "ThirdPartyOnboardingForm") }},

    {{ col_rename("IsArchived", "ThirdPartyOnboardingForm") }},
    {{ col_rename("SubmissionCount", "ThirdPartyOnboardingForm") }},
    {{ col_rename("Status", "ThirdPartyOnboardingForm") }},
    {{ col_rename("CreatedFromFormId", "ThirdPartyOnboardingForm") }}
from base

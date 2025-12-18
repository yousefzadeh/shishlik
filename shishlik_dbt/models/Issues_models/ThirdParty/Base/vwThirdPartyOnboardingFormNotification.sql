{{ config(materialized="view") }}
with
    base as (
        select Id, TenantId, CreationTime, LastModificationTime, UserId, OrganizationUnitId, FormId, AssessmentId, NotificationUserType,
        case when NotificationUserType = 1 then 'Form Submission User'
        when NotificationUserType = 2 then 'Assessment Completion User'
        when NotificationUserType = 3 then 'ThirdParty Owners' end NotificationUserTypeCode
        from {{ source("issue_models", "ThirdPartyOnboardingFormNotification") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ThirdPartyOnboardingFormNotification") }},
    {{ col_rename("CreationTime", "ThirdPartyOnboardingFormNotification") }},
    {{ col_rename("LastModificationTime", "ThirdPartyOnboardingFormNotification") }},
    {{ col_rename("TenantId", "ThirdPartyOnboardingFormNotification") }},
    {{ col_rename("FormId", "ThirdPartyOnboardingFormNotification") }},
    {{ col_rename("UserId", "ThirdPartyOnboardingFormNotification") }},

    {{ col_rename("OrganizationUnitId", "ThirdPartyOnboardingFormNotification") }},
    {{ col_rename("AssessmentId", "ThirdPartyOnboardingFormNotification") }},
    {{ col_rename("NotificationUserType", "ThirdPartyOnboardingFormNotification") }},
    {{ col_rename("NotificationUserTypeCode", "ThirdPartyOnboardingFormNotification") }}
from base

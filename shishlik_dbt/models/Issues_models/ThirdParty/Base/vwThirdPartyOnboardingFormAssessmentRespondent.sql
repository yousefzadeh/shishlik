{{ config(materialized="view") }}
with
    base as (
        select Id, TenantId, CreationTime, LastModificationTime, UserId, AllowSubmission, AssessmentId, AssessmentTemplateId, FormId
        from {{ source("issue_models", "ThirdPartyOnboardingFormAssessmentRespondent") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ThirdPartyOnboardingFormAssessmentRespondent") }},
    {{ col_rename("CreationTime", "ThirdPartyOnboardingFormAssessmentRespondent") }},
    {{ col_rename("LastModificationTime", "ThirdPartyOnboardingFormAssessmentRespondent") }},
    {{ col_rename("TenantId", "ThirdPartyOnboardingFormAssessmentRespondent") }},
    {{ col_rename("FormId", "ThirdPartyOnboardingFormAssessmentRespondent") }},
    {{ col_rename("UserId", "ThirdPartyOnboardingFormAssessmentRespondent") }},

    {{ col_rename("AllowSubmission", "ThirdPartyOnboardingFormAssessmentRespondent") }},
    {{ col_rename("AssessmentId", "ThirdPartyOnboardingFormAssessmentRespondent") }},
    {{ col_rename("AssessmentTemplateId", "ThirdPartyOnboardingFormAssessmentRespondent") }}
from base

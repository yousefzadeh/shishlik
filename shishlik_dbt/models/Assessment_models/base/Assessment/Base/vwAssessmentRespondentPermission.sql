{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [AssessmentId], [UserId], [CanSubmitAssessment]

        from {{ source("assessment_models", "AssessmentRespondentPermission") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentRespondentPermission") }},
    {{ col_rename("TenantId", "AssessmentRespondentPermission") }},
    {{ col_rename("AssessmentId", "AssessmentRespondentPermission") }},
    {{ col_rename("CreationTime", "AssessmentRespondentPermission") }},
    {{ col_rename("LastModificationTime", "AssessmentRespondentPermission") }},
    {{ col_rename("UserId", "AssessmentRespondentPermission") }},

    {{ col_rename("CanSubmitAssessment", "AssessmentRespondentPermission") }}
from base

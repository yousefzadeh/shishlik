{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [LastAccessed], [AssessmentId], [UserId], [TenantId]
        from {{ source("assessment_models", "AssessmentAccess") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentAccess") }},
    {{ col_rename("LastAccessed", "AssessmentAccess") }},
    {{ col_rename("AssessmentId", "AssessmentAccess") }},
    {{ col_rename("UserId", "AssessmentAccess") }},

    {{ col_rename("TenantId", "AssessmentAccess") }}
from base

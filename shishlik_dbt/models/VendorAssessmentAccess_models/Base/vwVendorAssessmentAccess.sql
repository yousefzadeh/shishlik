{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [LastAccessed], [AssessmentId], [UserId], [TenantId]
        from {{ source("VendorAssessmentAccess_models", "VendorAssessmentAccess") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "VendorAssessmentAccess") }},
    {{ col_rename("LastAccessed", "VendorAssessmentAccess") }},
    {{ col_rename("AssessmentId", "VendorAssessmentAccess") }},
    {{ col_rename("UserId", "VendorAssessmentAccess") }},

    {{ col_rename("TenantId", "VendorAssessmentAccess") }}
from base

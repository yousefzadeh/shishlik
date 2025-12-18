{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [CustomFieldId], [AssessmentId], [Order]

        from {{ source("assessment_models", "AssessmentCustomField") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentCustomField") }},
    {{ col_rename("TenantId", "AssessmentCustomField") }},
    {{ col_rename("CustomFieldId", "AssessmentCustomField") }},
    {{ col_rename("AssessmentId", "AssessmentCustomField") }},

    {{ col_rename("Order", "AssessmentCustomField") }}
from base

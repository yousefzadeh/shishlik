{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [AssessmentDomainId], [ControlsId], [TenantId], [IsDocumentUploadMandatory]
        from {{ source("assessment_models", "AssessmentDomainControl") }}
    {# {{ system_remove_IsDeleted() }} #}
    )

select
    {{ col_rename("ID", "AssessmentDomainControl") }},
    {{ col_rename("AssessmentDomainId", "AssessmentDomainControl") }},
    {{ col_rename("ControlsId", "AssessmentDomainControl") }},
    {{ col_rename("TenantId", "AssessmentDomainControl") }},

    {{ col_rename("IsDocumentUploadMandatory", "AssessmentDomainControl") }},
    {{ col_rename("IsDeleted", "AssessmentDomainControl") }}
from base

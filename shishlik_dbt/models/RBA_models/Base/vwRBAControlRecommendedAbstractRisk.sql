{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [AssessmentDomainControlId],
            [CustomFieldId],
            [CustomFieldAttributeId],
            [AbstractRiskId]
        from {{ source("rba_models", "RBAControlRecommendedAbstractRisk") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RBAControlRecommendedAbstractRisk") }},
    {{ col_rename("TenantId", "RBAControlRecommendedAbstractRisk") }},
    {{ col_rename("AssessmentDomainControlId", "RBAControlRecommendedAbstractRisk") }},
    {{ col_rename("CustomFieldId", "RBAControlRecommendedAbstractRisk") }},

    {{ col_rename("CustomFieldAttributeId", "RBAControlRecommendedAbstractRisk") }},
    {{ col_rename("AbstractRiskId", "RBAControlRecommendedAbstractRisk") }}
from base

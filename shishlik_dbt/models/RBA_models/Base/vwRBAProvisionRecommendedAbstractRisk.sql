{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [AssessmentDomainProvisionId],
            [CustomFieldId],
            [CustomFieldAttributeId],
            [AbstractRiskId]
        from {{ source("rba_models", "RBAProvisionRecommendedAbstractRisk") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RBAProvisionRecommendedAbstractRisk") }},
    {{ col_rename("TenantId", "RBAProvisionRecommendedAbstractRisk") }},
    {{ col_rename("AssessmentDomainProvisionId", "RBAProvisionRecommendedAbstractRisk") }},
    {{ col_rename("CustomFieldId", "RBAProvisionRecommendedAbstractRisk") }},

    {{ col_rename("CustomFieldAttributeId", "RBAProvisionRecommendedAbstractRisk") }},
    {{ col_rename("AbstractRiskId", "RBAProvisionRecommendedAbstractRisk") }}
from base

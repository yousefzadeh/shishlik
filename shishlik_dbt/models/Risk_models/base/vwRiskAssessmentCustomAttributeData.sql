{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [RiskAssessmentId], [ThirdPartyAttributesId]
        from {{ source("risk_models", "RiskAssessmentCustomAttributeData") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskAssessmentCustomAttributeData") }},
    {{ col_rename("CreationTime", "RiskAssessmentCustomAttributeData") }},
    {{ col_rename("CreatorUserId", "RiskAssessmentCustomAttributeData") }},
    {{ col_rename("LastModificationTime", "RiskAssessmentCustomAttributeData") }},

    {{ col_rename("LastModifierUserId", "RiskAssessmentCustomAttributeData") }},
    {{ col_rename("IsDeleted", "RiskAssessmentCustomAttributeData") }},
    {{ col_rename("DeleterUserId", "RiskAssessmentCustomAttributeData") }},
    {{ col_rename("DeletionTime", "RiskAssessmentCustomAttributeData") }},

    {{ col_rename("TenantId", "RiskAssessmentCustomAttributeData") }},
    {{ col_rename("RiskAssessmentId", "RiskAssessmentCustomAttributeData") }},
    {{ col_rename("ThirdPartyAttributesId", "RiskAssessmentCustomAttributeData") }}
from base

{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TagId], [RiskAssessmentId], [TenantId]
        from {{ source("risk_models", "RiskAssessmentTag") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskAssessmentTag") }},
    {{ col_rename("CreationTime", "RiskAssessmentTag") }},
    {{ col_rename("CreatorUserId", "RiskAssessmentTag") }},
    {{ col_rename("LastModificationTime", "RiskAssessmentTag") }},

    {{ col_rename("LastModifierUserId", "RiskAssessmentTag") }},
    {{ col_rename("IsDeleted", "RiskAssessmentTag") }},
    {{ col_rename("DeleterUserId", "RiskAssessmentTag") }},
    {{ col_rename("DeletionTime", "RiskAssessmentTag") }},

    {{ col_rename("TagId", "RiskAssessmentTag") }},
    {{ col_rename("RiskAssessmentId", "RiskAssessmentTag") }},
    {{ col_rename("TenantId", "RiskAssessmentTag") }}
from base

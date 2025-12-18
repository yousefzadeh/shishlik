{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [RiskAssessmentId],
            [ThirdPartyControlId],
            cast([TextData] as nvarchar(4000)) TextData,
            [TenantId],
            [CustomDateValue]
        from {{ source("risk_models", "RiskAssessmentThirdPartyControlFreeText") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskAssessmentThirdPartyControlFreeText") }},
    {{ col_rename("CreationTime", "RiskAssessmentThirdPartyControlFreeText") }},
    {{ col_rename("CreatorUserId", "RiskAssessmentThirdPartyControlFreeText") }},
    {{ col_rename("LastModificationTime", "RiskAssessmentThirdPartyControlFreeText") }},

    {{ col_rename("LastModifierUserId", "RiskAssessmentThirdPartyControlFreeText") }},
    {{ col_rename("IsDeleted", "RiskAssessmentThirdPartyControlFreeText") }},
    {{ col_rename("DeleterUserId", "RiskAssessmentThirdPartyControlFreeText") }},
    {{ col_rename("DeletionTime", "RiskAssessmentThirdPartyControlFreeText") }},

    {{ col_rename("RiskAssessmentId", "RiskAssessmentThirdPartyControlFreeText") }},
    {{ col_rename("ThirdPartyControlId", "RiskAssessmentThirdPartyControlFreeText") }},
    {{ col_rename("TextData", "RiskAssessmentThirdPartyControlFreeText") }},
    {{ col_rename("TenantId", "RiskAssessmentThirdPartyControlFreeText") }},

    {{ col_rename("CustomDateValue", "RiskAssessmentThirdPartyControlFreeText") }}
from base

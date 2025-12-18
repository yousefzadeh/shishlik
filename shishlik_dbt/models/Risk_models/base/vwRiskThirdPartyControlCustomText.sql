{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [RiskId],
            [ThirdPartyControlId],
            cast([TextData] as nvarchar(4000)) TextData,
            [TenantId],
            [CustomDateValue],
            [NumberValue],
            CONCAT(RiskId, ThirdPartyControlId) as PK
        from {{ source("risk_models", "RiskThirdPartyControlCustomText") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskThirdPartyControlCustomText") }},
    {{ col_rename("CreationTime", "RiskThirdPartyControlCustomText") }},
    {{ col_rename("CreatorUserId", "RiskThirdPartyControlCustomText") }},
    {{ col_rename("LastModificationTime", "RiskThirdPartyControlCustomText") }},

    {{ col_rename("LastModifierUserId", "RiskThirdPartyControlCustomText") }},
    {{ col_rename("IsDeleted", "RiskThirdPartyControlCustomText") }},
    {{ col_rename("DeleterUserId", "RiskThirdPartyControlCustomText") }},
    {{ col_rename("DeletionTime", "RiskThirdPartyControlCustomText") }},

    {{ col_rename("RiskId", "RiskThirdPartyControlCustomText") }},
    {{ col_rename("ThirdPartyControlId", "RiskThirdPartyControlCustomText") }},
    {{ col_rename("TextData", "RiskThirdPartyControlCustomText") }},
    {{ col_rename("TenantId", "RiskThirdPartyControlCustomText") }},

    {{ col_rename("CustomDateValue", "RiskThirdPartyControlCustomText") }},
    {{ col_rename("NumberValue", "RiskThirdPartyControlCustomText") }},
    {{ col_rename("PK", "RiskThirdPartyControlCustomText") }}
from base

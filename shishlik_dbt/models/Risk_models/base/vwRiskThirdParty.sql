{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [RiskId], [TenantVendorId]
        from {{ source("risk_models", "RiskThirdParty") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskThirdParty") }},
    {{ col_rename("CreationTime", "RiskThirdParty") }},
    {{ col_rename("CreatorUserId", "RiskThirdParty") }},
    {{ col_rename("LastModificationTime", "RiskThirdParty") }},

    {{ col_rename("LastModifierUserId", "RiskThirdParty") }},
    {{ col_rename("IsDeleted", "RiskThirdParty") }},
    {{ col_rename("DeleterUserId", "RiskThirdParty") }},
    {{ col_rename("DeletionTime", "RiskThirdParty") }},

    {{ col_rename("TenantId", "RiskThirdParty") }},
    {{ col_rename("RiskId", "RiskThirdParty") }},
    {{ col_rename("TenantVendorId", "RiskThirdParty") }}
from base

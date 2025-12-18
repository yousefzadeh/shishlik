{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [RiskId], [RegisterRecordId]
        from {{ source("risk_models", "RiskRegisterRecord") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskRegisterRecord") }},
    {{ col_rename("CreationTime", "RiskRegisterRecord") }},
    {{ col_rename("CreatorUserId", "RiskRegisterRecord") }},
    {{ col_rename("LastModificationTime", "RiskRegisterRecord") }},

    {{ col_rename("LastModifierUserId", "RiskRegisterRecord") }},
    {{ col_rename("IsDeleted", "RiskRegisterRecord") }},
    {{ col_rename("DeleterUserId", "RiskRegisterRecord") }},
    {{ col_rename("DeletionTime", "RiskRegisterRecord") }},

    {{ col_rename("TenantId", "RiskRegisterRecord") }},
    {{ col_rename("RiskId", "RiskRegisterRecord") }},
    {{ col_rename("RegisterRecordId", "RiskRegisterRecord") }}
from base

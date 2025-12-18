{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [AuthorityProvisionId], [RiskId], [TenantId]
        from {{ source("risk_models", "RiskProvision") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskProvision") }},
    {{ col_rename("CreationTime", "RiskProvision") }},
    {{ col_rename("CreatorUserId", "RiskProvision") }},
    {{ col_rename("LastModificationTime", "RiskProvision") }},

    {{ col_rename("LastModifierUserId", "RiskProvision") }},
    {{ col_rename("IsDeleted", "RiskProvision") }},
    {{ col_rename("DeleterUserId", "RiskProvision") }},
    {{ col_rename("DeletionTime", "RiskProvision") }},

    {{ col_rename("AuthorityProvisionId", "RiskProvision") }},
    {{ col_rename("RiskId", "RiskProvision") }},
    {{ col_rename("TenantId", "RiskProvision") }}
from base

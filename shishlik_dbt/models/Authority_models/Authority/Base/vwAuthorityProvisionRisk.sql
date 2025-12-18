{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [AuthorityProvisionId], [RiskStatus]
        from {{ source("assessment_models", "AuthorityProvisionRisk") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AuthorityProvisionRisk") }},
    {{ col_rename("CreationTime", "AuthorityProvisionRisk") }},
    {{ col_rename("CreatorUserId", "AuthorityProvisionRisk") }},
    {{ col_rename("LastModificationTime", "AuthorityProvisionRisk") }},

    {{ col_rename("LastModifierUserId", "AuthorityProvisionRisk") }},
    {{ col_rename("IsDeleted", "AuthorityProvisionRisk") }},
    {{ col_rename("DeleterUserId", "AuthorityProvisionRisk") }},
    {{ col_rename("DeletionTime", "AuthorityProvisionRisk") }},

    {{ col_rename("TenantId", "AuthorityProvisionRisk") }},
    {{ col_rename("AuthorityProvisionId", "AuthorityProvisionRisk") }},
    {{ col_rename("RiskStatus", "AuthorityProvisionRisk") }}
from base

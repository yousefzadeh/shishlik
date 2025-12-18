{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [SourceAuthorityProvisionId],
            [TargetAuthorityProvisionId],
            [Similarity]
        from {{ source("tenant_models", "TenantAuthorityProvisionMapping") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "TenantAuthorityProvisionMapping") }},
    {{ col_rename("TenantId", "TenantAuthorityProvisionMapping") }},
    {{ col_rename("SourceAuthorityProvisionId", "TenantAuthorityProvisionMapping") }},
    {{ col_rename("TargetAuthorityProvisionId", "TenantAuthorityProvisionMapping") }},

    {{ col_rename("Similarity", "TenantAuthorityProvisionMapping") }}
from base

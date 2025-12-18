{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [TargetAuthorityId],
            [SimilarityPercentage],
            [SourceTenantAuthorityId],
            [SourceAuthorityId]
        from {{ source("tenant_models", "TenantAuthorityMapping") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "TenantAuthorityMapping") }},
    {{ col_rename("TenantId", "TenantAuthorityMapping") }},
    {{ col_rename("TargetAuthorityId", "TenantAuthorityMapping") }},
    {{ col_rename("SimilarityPercentage", "TenantAuthorityMapping") }},

    {{ col_rename("SourceTenantAuthorityId", "TenantAuthorityMapping") }},
    {{ col_rename("SourceAuthorityId", "TenantAuthorityMapping") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [AuthorityId], [PolicyId], [SimilarityPercentage]
        from {{ source("assessment_models", "AuthorityPolicy") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AuthorityPolicy") }},
    {{ col_rename("CreationTime", "AuthorityPolicy") }},
    {{ col_rename("CreatorUserId", "AuthorityPolicy") }},
    {{ col_rename("LastModificationTime", "AuthorityPolicy") }},

    {{ col_rename("LastModifierUserId", "AuthorityPolicy") }},
    {{ col_rename("IsDeleted", "AuthorityPolicy") }},
    {{ col_rename("DeleterUserId", "AuthorityPolicy") }},
    {{ col_rename("DeletionTime", "AuthorityPolicy") }},

    {{ col_rename("TenantId", "AuthorityPolicy") }},
    {{ col_rename("AuthorityId", "AuthorityPolicy") }},
    {{ col_rename("PolicyId", "AuthorityPolicy") }},
    {{ col_rename("SimilarityPercentage", "AuthorityPolicy") }}
from base

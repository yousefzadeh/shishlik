{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [SourceAuthorityProvisionId],
            [TargetAuthorityProvisionId],
            [Match],
            cast([RowKeyInHailey] as nvarchar(4000)) RowKeyInHailey
        from {{ source("assessment_models", "AuthorityProvisionSimilarity") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AuthorityProvisionSimilarity") }},
    {{ col_rename("CreationTime", "AuthorityProvisionSimilarity") }},
    {{ col_rename("CreatorUserId", "AuthorityProvisionSimilarity") }},
    {{ col_rename("LastModificationTime", "AuthorityProvisionSimilarity") }},

    {{ col_rename("LastModifierUserId", "AuthorityProvisionSimilarity") }},
    {{ col_rename("IsDeleted", "AuthorityProvisionSimilarity") }},
    {{ col_rename("DeleterUserId", "AuthorityProvisionSimilarity") }},
    {{ col_rename("DeletionTime", "AuthorityProvisionSimilarity") }},

    {{ col_rename("SourceAuthorityProvisionId", "AuthorityProvisionSimilarity") }},
    {{ col_rename("TargetAuthorityProvisionId", "AuthorityProvisionSimilarity") }},
    {{ col_rename("Match", "AuthorityProvisionSimilarity") }},
    {{ col_rename("RowKeyInHailey", "AuthorityProvisionSimilarity") }}
from base

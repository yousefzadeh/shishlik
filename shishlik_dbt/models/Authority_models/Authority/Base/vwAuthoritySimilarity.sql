{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [SourceAuthorityId],
            [TargetAuthorityId],
            cast([SimilarityPercentage] as nvarchar(4000)) SimilarityPercentage
        from {{ source("assessment_models", "AuthoritySimilarity") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AuthoritySimilarity") }},
    {{ col_rename("CreationTime", "AuthoritySimilarity") }},
    {{ col_rename("CreatorUserId", "AuthoritySimilarity") }},
    {{ col_rename("LastModificationTime", "AuthoritySimilarity") }},

    {{ col_rename("LastModifierUserId", "AuthoritySimilarity") }},
    {{ col_rename("IsDeleted", "AuthoritySimilarity") }},
    {{ col_rename("DeleterUserId", "AuthoritySimilarity") }},
    {{ col_rename("DeletionTime", "AuthoritySimilarity") }},

    {{ col_rename("SourceAuthorityId", "AuthoritySimilarity") }},
    {{ col_rename("TargetAuthorityId", "AuthoritySimilarity") }},
    {{ col_rename("SimilarityPercentage", "AuthoritySimilarity") }}
from base

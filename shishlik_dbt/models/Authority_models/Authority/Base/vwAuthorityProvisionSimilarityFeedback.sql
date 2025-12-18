{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [TenantId],
            [SourceAuthorityProvisionId],
            [TargetAuthorityProvisionId],
            [Match],
            [PublishedToHailey]
        from {{ source("assessment_models", "AuthorityProvisionSimilarityFeedback") }}
    )

select
    {{ col_rename("Id", "AuthorityProvisionSimilarityFeedback") }},
    {{ col_rename("CreationTime", "AuthorityProvisionSimilarityFeedback") }},
    {{ col_rename("CreatorUserId", "AuthorityProvisionSimilarityFeedback") }},
    {{ col_rename("TenantId", "AuthorityProvisionSimilarityFeedback") }},

    {{ col_rename("SourceAuthorityProvisionId", "AuthorityProvisionSimilarityFeedback") }},
    {{ col_rename("TargetAuthorityProvisionId", "AuthorityProvisionSimilarityFeedback") }},
    {{ col_rename("Match", "AuthorityProvisionSimilarityFeedback") }},
    {{ col_rename("PublishedToHailey", "AuthorityProvisionSimilarityFeedback") }}
from base

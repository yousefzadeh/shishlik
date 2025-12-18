{{ config(materialized="view") }}
with
    base as (
        select
            [Id], [CreationTime], [CreatorUserId], [TenantId], [ControlId], [ProvisionId], [Match], [PublishedToHailey]
        from {{ source("assessment_models", "ControlProvisionSimilarityFeedback") }}
    )

select
    {{ col_rename("Id", "ControlProvisionSimilarityFeedback") }},
    {{ col_rename("TenantId", "ControlProvisionSimilarityFeedback") }},
    {{ col_rename("ControlId", "ControlProvisionSimilarityFeedback") }},
    {{ col_rename("ProvisionId", "ControlProvisionSimilarityFeedback") }},

    {{ col_rename("Match", "ControlProvisionSimilarityFeedback") }},
    {{ col_rename("PublishedToHailey", "ControlProvisionSimilarityFeedback") }}
from base

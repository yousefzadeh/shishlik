{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [EditionId],
            [ContentId],
            cast([Feature] as nvarchar(4000)) Feature,
            [FeatureContentType]
        from {{ source("edition_models", "EditionFeatureContent") }}
    )

select
    {{ col_rename("Id", "EditionFeatureContent") }},
    {{ col_rename("EditionId", "EditionFeatureContent") }},
    {{ col_rename("ContentId", "EditionFeatureContent") }},
    {{ col_rename("Feature", "EditionFeatureContent") }},

    {{ col_rename("FeatureContentType", "EditionFeatureContent") }}
from base

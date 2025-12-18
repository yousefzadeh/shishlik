{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, 
        [TenantId], 
        [UserId], 
        [AssessmentId], 
        [IsFavourite],
        cast(coalesce([LastModificationTime],[CreationTime]) as datetime2) as UpdateTime
        from {{ source("assessment_models", "UserFavouriteAssessment") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "UserFavouriteAssessment") }},
    {{ col_rename("TenantId", "UserFavouriteAssessment") }},
    {{ col_rename("UserId", "UserFavouriteAssessment") }},
    {{ col_rename("AssessmentId", "UserFavouriteAssessment") }},
    {{ col_rename("IsFavourite", "UserFavouriteAssessment") }},
    {{ col_rename("UpdateTime", "UserFavouriteAssessment") }}
from base

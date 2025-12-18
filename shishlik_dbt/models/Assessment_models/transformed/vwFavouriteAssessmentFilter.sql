with
    base as (
        select distinct 
        [TenantId], 
        [AssessmentId], 
        [IsFavourite],
        cast(coalesce([LastModificationTime],[CreationTime]) as datetime2) as UpdateTime
        from {{ source("assessment_models", "UserFavouriteAssessment") }} {{ system_remove_IsDeleted() }}
    )
select
    {{ col_rename("TenantId", "FavouriteAssessmentFilter") }},
    {{ col_rename("AssessmentId", "FavouriteAssessmentFilter") }},
    {{ col_rename("IsFavourite", "FavouriteAssessmentFilter") }},
    {{ col_rename("UpdateTime", "FavouriteAssessmentFilter") }}
from base
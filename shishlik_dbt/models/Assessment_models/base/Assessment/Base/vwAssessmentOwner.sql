{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }},
         [TenantId],
         [AssessmentId], 
         [UserId], 
         [OrganizationUnitId],
         coalesce(LastModificationTime,CreationTime) as UpdateTime
        from {{ source("assessment_models", "AssessmentOwner") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentOwner") }},
    {{ col_rename("TenantId", "AssessmentOwner") }},
    {{ col_rename("AssessmentId", "AssessmentOwner") }},
    {{ col_rename("CreationTime", "AssessmentOwner") }},
    {{ col_rename("LastModificationTime", "AssessmentOwner") }},
    {{ col_rename("UserId", "AssessmentOwner") }},
    {{ col_rename("OrganizationUnitId", "AssessmentOwner") }},
    {{ col_rename("UpdateTime", "AssessmentOwner") }}
from base

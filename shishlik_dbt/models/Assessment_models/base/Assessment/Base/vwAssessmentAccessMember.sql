{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}
        , [AssessmentId]
        , [UserId]
        , [OrganizationUnitId]
        , [TenantId]
        , coalesce(LastModificationTime,CreationTime ) as UpdateTime
        from {{ source("assessment_models", "AssessmentAccessMember") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentAccessMember") }},
    {{ col_rename("AssessmentId", "AssessmentAccessMember") }},
    {{ col_rename("CreationTime", "AssessmentAccessMember") }},
    {{ col_rename("LastModificationTime", "AssessmentAccessMember") }},
    {{ col_rename("UserId", "AssessmentAccessMember") }},
    {{ col_rename("OrganizationUnitId", "AssessmentAccessMember") }},
    {{ col_rename("TenantId", "AssessmentAccessMember") }},
    {{ col_rename("UpdateTime", "AssessmentAccessMember") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select Uuid,
            Id,
            CreationTime,
            CreatorUserId,
            [TenantId], 
            [AssessmentId],
            [RegisterItemId],
            coalesce([LastModificationTime],[CreationTime]) as UpdateTime
        from {{ source("assessment_models", "AssessmentScopeRegisterItem") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Uuid", "AssessmentScopeRegisterItem") }},
    {{ col_rename("Id", "AssessmentScopeRegisterItem") }},
    {{ col_rename("TenantId", "AssessmentScopeRegisterItem") }},
    {{ col_rename("RegisterItemId", "AssessmentScopeRegisterItem") }},
    {{ col_rename("AssessmentId", "AssessmentScopeRegisterItem") }},
    {{ col_rename("UpdateTime", "AssessmentScopeRegisterItem") }}
from base

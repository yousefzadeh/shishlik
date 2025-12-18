{{ config(materialized="view") }}
with
    base as (
        select Uuid,
            Id,
            CreationTime,
            CreatorUserId,
            [TenantId], 
            [AssessmentId],
            [RiskId],
            coalesce([LastModificationTime],[CreationTime]) as UpdateTime
        from {{ source("assessment_models", "AssessmentScopeRisk") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Uuid", "AssessmentScopeRisk") }},
    {{ col_rename("Id", "AssessmentScopeRisk") }},
    {{ col_rename("TenantId", "AssessmentScopeRisk") }},
    {{ col_rename("RiskId", "AssessmentScopeRisk") }},
    {{ col_rename("AssessmentId", "AssessmentScopeRisk") }},
    {{ col_rename("UpdateTime", "AssessmentScopeRisk") }}
from base

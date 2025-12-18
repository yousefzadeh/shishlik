{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, 
            [TenantId], 
            [RiskId], 
            [AssessmentId], 
            [AssessmentDomainId], 
            [QuestionId],
            coalesce([LastModificationTime],[CreationTime]) as UpdateTime
        from {{ source("assessment_models", "AssessmentRisk") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentRisk") }},
    {{ col_rename("TenantId", "AssessmentRisk") }},
    {{ col_rename("RiskId", "AssessmentRisk") }},
    {{ col_rename("AssessmentId", "AssessmentRisk") }},
    {{ col_rename("AssessmentDomainId", "AssessmentRisk") }},
    {{ col_rename("QuestionId", "AssessmentRisk") }},
    {{ col_rename("UpdateTime", "AssessmentRisk") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Name] as varchar(200))[Name],
            [AssessmentId],
            [TenantId],
            cast([IntroductionText] as nvarchar(4000)) IntroductionText,
            [Order],
            -- Estimated PK,
            coalesce(LastModificationTime, CreationTime) UpdateTime,
            cast(CONCAT(AssessmentID, TenantId, Name) as nvarchar(4000)) as PK
        from {{ source("assessment_models", "AssessmentDomain") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentDomain") }},
    {{ col_rename("Name", "AssessmentDomain") }},
    {{ col_rename("AssessmentId", "AssessmentDomain") }},
    {{ col_rename("TenantId", "AssessmentDomain") }},

    {{ col_rename("IntroductionText", "AssessmentDomain") }},
    {{ col_rename("Order", "AssessmentDomain") }},
    {{ col_rename("UpdateTime", "AssessmentDomain") }},
    {{ col_rename("PK", "AssessmentDomain") }}
from base
group by [ID], [Name], [AssessmentId], [TenantId], [IntroductionText], [Order], [UpdateTime], [PK]

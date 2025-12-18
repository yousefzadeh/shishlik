{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [UserId],
            [AssessmentDomainControlId],
            cast([Note] as nvarchar(4000)) Note
        from {{ source("assessment_models", "AssessmentDomainControlNote") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentDomainControlNote") }},
    {{ col_rename("TenantId", "AssessmentDomainControlNote") }},
    {{ col_rename("UserId", "AssessmentDomainControlNote") }},
    {{ col_rename("AssessmentDomainControlId", "AssessmentDomainControlNote") }},

    {{ col_rename("Note", "AssessmentDomainControlNote") }}
from base

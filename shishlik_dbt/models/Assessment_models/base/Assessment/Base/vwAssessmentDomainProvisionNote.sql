{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [UserId],
            [AssessmentDomainProvisionId],
            cast([Note] as nvarchar(4000)) Note
        from {{ source("assessment_models", "AssessmentDomainProvisionNote") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AssessmentDomainProvisionNote") }},
    {{ col_rename("TenantId", "AssessmentDomainProvisionNote") }},
    {{ col_rename("UserId", "AssessmentDomainProvisionNote") }},
    {{ col_rename("AssessmentDomainProvisionId", "AssessmentDomainProvisionNote") }},

    {{ col_rename("Note", "AssessmentDomainProvisionNote") }}
from base

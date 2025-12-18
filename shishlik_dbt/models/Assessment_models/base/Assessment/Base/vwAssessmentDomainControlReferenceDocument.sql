{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [FileName],
            [DisplayFileName],
            [ContainerName],
            [FileSizeInKB],
            [AssessmentDomainControlId],
            [TenantId]
        from
            {{ source("assessment_models", "AssessmentDomainControlReferenceDocument") }}
            {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentDomainControlReferenceDocument") }},
    {{ col_rename("FileName", "AssessmentDomainControlReferenceDocument") }},
    {{ col_rename("DisplayFileName", "AssessmentDomainControlReferenceDocument") }},
    {{ col_rename("ContainerName", "AssessmentDomainControlReferenceDocument") }},

    {{ col_rename("FileSizeInKB", "AssessmentDomainControlReferenceDocument") }},
    {{ col_rename("AssessmentDomainControlId", "AssessmentDomainControlReferenceDocument") }},
    {{ col_rename("TenantId", "AssessmentDomainControlReferenceDocument") }}
from base

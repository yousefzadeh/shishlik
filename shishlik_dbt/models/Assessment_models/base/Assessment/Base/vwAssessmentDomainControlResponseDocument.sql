{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([FileName] as nvarchar(4000)) FileName,
            cast([DisplayFileName] as nvarchar(4000)) DisplayFileName,
            cast([ContainerName] as nvarchar(4000)) ContainerName,
            [FileSizeInKB],
            [TenantId],
            [AssessmentDomainControlId],
            [AssessmentResponseId]
        from
            {{ source("assessment_models", "AssessmentDomainControlResponseDocument") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentDomainControlResponseDocument") }},
    {{ col_rename("FileName", "AssessmentDomainControlResponseDocument") }},
    {{ col_rename("DisplayFileName", "AssessmentDomainControlResponseDocument") }},
    {{ col_rename("ContainerName", "AssessmentDomainControlResponseDocument") }},

    {{ col_rename("FileSizeInKB", "AssessmentDomainControlResponseDocument") }},
    {{ col_rename("TenantId", "AssessmentDomainControlResponseDocument") }},
    {{ col_rename("AssessmentDomainControlId", "AssessmentDomainControlResponseDocument") }},
    {{ col_rename("AssessmentResponseId", "AssessmentDomainControlResponseDocument") }}
from base

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
            [AssessmentDomainProvisionId],
            [AssessmentResponseId]
        from
            {{ source("assessment_models", "AssessmentDomainProvisionResponseDocument") }}
            {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentDomainProvisionResponseDocument") }},
    {{ col_rename("FileName", "AssessmentDomainProvisionResponseDocument") }},
    {{ col_rename("DisplayFileName", "AssessmentDomainProvisionResponseDocument") }},
    {{ col_rename("ContainerName", "AssessmentDomainProvisionResponseDocument") }},

    {{ col_rename("FileSizeInKB", "AssessmentDomainProvisionResponseDocument") }},
    {{ col_rename("TenantId", "AssessmentDomainProvisionResponseDocument") }},
    {{ col_rename("AssessmentDomainProvisionId", "AssessmentDomainProvisionResponseDocument") }},
    {{ col_rename("AssessmentResponseId", "AssessmentDomainProvisionResponseDocument") }}
from base

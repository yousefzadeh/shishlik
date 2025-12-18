{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([FileName] as nvarchar(4000)) FileName,
            cast([DisplayFileName] as nvarchar(4000)) DisplayFileName,
            cast([ContainerName] as nvarchar(4000)) ContainerName,
            [FileSizeInKB],
            [AssessmentDomainProvisionId],
            [TenantId]
        from
            {{ source("assessment_models", "AssessmentDomainProvisionReferenceDocument") }}
            {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AssessmentDomainProvisionReferenceDocument") }},
    {{ col_rename("FileName", "AssessmentDomainProvisionReferenceDocument") }},
    {{ col_rename("DisplayFileName", "AssessmentDomainProvisionReferenceDocument") }},
    {{ col_rename("ContainerName", "AssessmentDomainProvisionReferenceDocument") }},

    {{ col_rename("FileSizeInKB", "AssessmentDomainProvisionReferenceDocument") }},
    {{ col_rename("AssessmentDomainProvisionId", "AssessmentDomainProvisionReferenceDocument") }},
    {{ col_rename("TenantId", "AssessmentDomainProvisionReferenceDocument") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([FileName] as nvarchar(4000)) FileName,
            cast([DisplayFileName] as nvarchar(4000)) DisplayFileName,
            cast([ContainerName] as nvarchar(4000)) ContainerName,
            [QuestionId],
            [TenantId],
            [FileSizeInKB],
            [QuestionGroupId]
        from {{ source("assessment_models", "QuestionReferenceDocuments") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "QuestionReferenceDocuments") }},
    {{ col_rename("FileName", "QuestionReferenceDocuments") }},
    {{ col_rename("DisplayFileName", "QuestionReferenceDocuments") }},
    {{ col_rename("ContainerName", "QuestionReferenceDocuments") }},

    {{ col_rename("QuestionId", "QuestionReferenceDocuments") }},
    {{ col_rename("TenantId", "QuestionReferenceDocuments") }},
    {{ col_rename("FileSizeInKB", "QuestionReferenceDocuments") }},
    {{ col_rename("QuestionGroupId", "QuestionReferenceDocuments") }}
from base

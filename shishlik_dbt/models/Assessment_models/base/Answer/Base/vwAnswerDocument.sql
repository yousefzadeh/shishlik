{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [AnswerId],
            cast([DocumentFileName] as nvarchar(4000)) DocumentFileName,
            cast([DisplayFileName] as nvarchar(4000)) DisplayFileName,
            cast([DocumentUrl] as nvarchar(4000)) DocumentUrl,
            [FileSizeInKB],
            [TenantId],
            cast(CONCAT(AnswerID, DocumentUrl) as nvarchar(4000)) as PK,
            coalesce(LastModificationTime,CreationTime) as UpdateTime
        from {{ source("assessment_models", "AnswerDocument") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AnswerDocument") }},
    {{ col_rename("AnswerId", "AnswerDocument") }},
    {{ col_rename("DocumentFileName", "AnswerDocument") }},
    {{ col_rename("DisplayFileName", "AnswerDocument") }},

    {{ col_rename("DocumentUrl", "AnswerDocument") }},
    {{ col_rename("TenantId", "AnswerDocument") }},
    {{ col_rename("PK", "AnswerDocument") }},
    {{ col_rename("UpdateTime", "AnswerDocument") }}
from
    base

    {# [Id]
,[CreationTime]
,[CreatorUserId]
,[LastModificationTime]
,[LastModifierUserId]
,[IsDeleted]
,[DeleterUserId]
,[DeletionTime] #}
    

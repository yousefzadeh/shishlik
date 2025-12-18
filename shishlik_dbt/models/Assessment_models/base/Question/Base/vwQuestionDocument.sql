{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([FileName] as nvarchar(4000)) FileName,
            [LastUpload],
            [UserId],
            cast([Fileurl] as nvarchar(4000)) Fileurl,
            [QuestionId],
            [TenantId],
            cast([DisplayFileName] as nvarchar(4000)) DisplayFileName
        from {{ source("assessment_models", "QuestionDocument") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "QuestionDocument") }},
    {{ col_rename("FileName", "QuestionDocument") }},
    {{ col_rename("LastUpload", "QuestionDocument") }},
    {{ col_rename("UserId", "QuestionDocument") }},

    {{ col_rename("Fileurl", "QuestionDocument") }},
    {{ col_rename("QuestionId", "QuestionDocument") }},
    {{ col_rename("TenantId", "QuestionDocument") }},
    {{ col_rename("DisplayFileName", "QuestionDocument") }}
from base

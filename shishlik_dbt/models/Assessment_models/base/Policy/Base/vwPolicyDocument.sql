{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([FileName] as nvarchar(4000)) FileName,
            cast([Fileurl] as nvarchar(4000)) Fileurl,
            [LastUpload],
            [PolicyId],
            [UserId],
            [TenantId]
        from {{ source("assessment_models", "PolicyDocument") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "PolicyDocument") }},
    {{ col_rename("FileName", "PolicyDocument") }},
    {{ col_rename("Fileurl", "PolicyDocument") }},
    {{ col_rename("LastUpload", "PolicyDocument") }},

    {{ col_rename("PolicyId", "PolicyDocument") }},
    {{ col_rename("UserId", "PolicyDocument") }},
    {{ col_rename("TenantId", "PolicyDocument") }}
from base

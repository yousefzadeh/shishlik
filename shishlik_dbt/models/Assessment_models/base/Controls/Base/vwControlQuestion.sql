{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}
        , [ControlsId]
        , [QuestionId]
        , [TenantId]
        , coalesce([LastModificationTime],[CreationTime]) as [CQ_UpdateTime]
        from {{ source("assessment_models", "ControlQuestion") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ControlQuestion") }},
    {{ col_rename("ControlsId", "ControlQuestion") }},
    {{ col_rename("QuestionId", "ControlQuestion") }},
    {{ col_rename("TenantId", "ControlQuestion") }},
    {{ col_rename("CQ_UpdateTime", "ControlQuestion") }}
from base

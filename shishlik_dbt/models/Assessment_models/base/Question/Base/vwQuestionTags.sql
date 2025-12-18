{{ config(materialized="view") }}
with
    base as (
         select [Id]
        , [CreationTime]
        , [CreatorUserId]
        , [TenantId]
        , [QuestionId]
        , [TagId]
		, coalesce([LastModificationTime],[CreationTime]) as [UpdateTime]
        from {{ source("assessment_models", "QuestionTags") }}
    )

select
    {{ col_rename("ID", "QuestionTags") }},
    {{ col_rename("CreationTime", "QuestionTags") }},
    {{ col_rename("CreatorUserId", "QuestionTags") }},
    {{ col_rename("TenantId", "QuestionTags") }},

    {{ col_rename("QuestionId", "QuestionTags") }},
    {{ col_rename("TagId", "QuestionTags") }},
    {{ col_rename("UpdateTime", "QuestionTags") }}
from base

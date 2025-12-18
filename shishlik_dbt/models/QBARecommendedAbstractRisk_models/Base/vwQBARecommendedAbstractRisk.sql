{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [QuestionId],
            cast([AnswerOption] as nvarchar(4000)) AnswerOption,
            [AbstractRiskId]
        from
            {{ source("QBARecommendedAbstractRisk_models", "QBARecommendedAbstractRisk") }}
            {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "QBARecommendedAbstractRisk") }},
    {{ col_rename("TenantId", "QBARecommendedAbstractRisk") }},
    {{ col_rename("QuestionId", "QBARecommendedAbstractRisk") }},
    {{ col_rename("AnswerOption", "QBARecommendedAbstractRisk") }},

    {{ col_rename("AbstractRiskId", "QBARecommendedAbstractRisk") }}
from base

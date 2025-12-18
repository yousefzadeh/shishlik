{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [ChangeTime],
            [ChangeType],
            [EntityChangeSetId],
            [EntityId],
            [EntityTypeFullName],
            [TenantId],
            case
                when row_number() over (partition by TenantId, EntityId order by ChangeTime desc) = 1
                then ChangeTime
                else NULL
            end CurrentDate
        from {{ source("assessment_models", "AbpEntityChanges") }}
    )

select
    {{ col_rename("Id", "AbpEntityChanges") }},
    {{ col_rename("ChangeTime", "AbpEntityChanges") }},
    {{ col_rename("ChangeType", "AbpEntityChanges") }},
    {{ col_rename("EntityChangeSetId", "AbpEntityChanges") }},

    {{ col_rename("EntityId", "AbpEntityChanges") }},
    {{ col_rename("EntityTypeFullName", "AbpEntityChanges") }},
    {{ col_rename("TenantId", "AbpEntityChanges") }},
    {{ col_rename("CurrentDate", "AbpEntityChanges") }}
from base

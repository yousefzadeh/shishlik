{{ config(materialized="view") }}
with
    base as (
        select
            Id,
            CreationTime,
            CreatorUserId,
            LastModificationTime,
            LastModifierUserId,
            DeleterUserId,
            DeletionTime,
            cast([Name] as nvarchar(4000))[Name],
            cast([Description] as nvarchar(4000)) Description,
            [TenantId],
            Explanation,
            [Type],
            case [Type] when 11 then 'Parent Child' when 12 then 'Looped' end [TypeCode],
            Compliance,
            case Compliance when 0 then 'None' when 1 then 'Compliant' when 2 then 'Not compliant' when 3 then 'Partially compliant' else 'Undefined' end as ComplianceCode,
            Code,
            cast(coalesce(LastModificationTime,CreationTime) as datetime2) as UpdateTime
        from {{ source("assessment_models", "QuestionGroup") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "QuestionGroup") }},
    {{ col_rename("CreationTime", "QuestionGroup") }},
    {{ col_rename("CreatorUserId", "QuestionGroup") }},

    {{ col_rename("LastModifierUserId", "QuestionGroup") }},
    {{ col_rename("DeleterUserId", "QuestionGroup") }},
    {{ col_rename("DeletionTime", "QuestionGroup") }},
    {{ col_rename("Name", "QuestionGroup") }},

    {{ col_rename("Description", "QuestionGroup") }},
    {{ col_rename("TenantId", "QuestionGroup") }},
    {{ col_rename("Explanation", "QuestionGroup") }},
    {{ col_rename("Type", "QuestionGroup") }},
    {{ col_rename("TypeCode", "QuestionGroup") }},

    {{ col_rename("Compliance", "QuestionGroup") }},
    {{ col_rename("ComplianceCode", "QuestionGroup") }},
    {{ col_rename("Code", "QuestionGroup") }},
    {{ col_rename("UpdateTime", "QuestionGroup") }}
from base

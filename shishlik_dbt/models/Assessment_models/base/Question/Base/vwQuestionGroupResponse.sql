{{ config(materialized="view") }}
with
    base as (
        select
            Id,
            TenantId,
            Response,
            QuestionGroupId,
            [Order],
            IdRef,
            CreationTime,
            CreatorUserId,
            LastModificationTime,
            LastModifierUserId,
            DeleterUserId,
            DeletionTime,
            AssessmentResponseId,
            Compliance,
            Code,
            cast(coalesce(LastModificationTime,CreationTime) as datetime2) as UpdateTime
        from {{ source("assessment_models", "QuestionGroupResponse") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "QuestionGroupResponse") }},
    {{ col_rename("TenantId", "QuestionGroupResponse") }},
    {{ col_rename("Response", "QuestionGroupResponse") }},
    {{ col_rename("QuestionGroupId", "QuestionGroupResponse") }},

    {{ col_rename("Order", "QuestionGroupResponse") }},
    {{ col_rename("IdRef", "QuestionGroupResponse") }},
    {{ col_rename("CreationTime", "QuestionGroupResponse") }},
    {{ col_rename("CreatorUserId", "QuestionGroupResponse") }},

    {{ col_rename("LastModificationTime", "QuestionGroupResponse") }},
    {{ col_rename("LastModifierUserId", "QuestionGroupResponse") }},
    {{ col_rename("DeleterUserId", "QuestionGroupResponse") }},
    {{ col_rename("DeletionTime", "QuestionGroupResponse") }},

    {{ col_rename("AssessmentResponseId", "QuestionGroupResponse") }},
    {{ col_rename("Compliance", "QuestionGroupResponse") }},
    {{ col_rename("Code", "QuestionGroupResponse") }},
    {{ col_rename("UpdateTime", "QuestionGroupResponse") }}
from base

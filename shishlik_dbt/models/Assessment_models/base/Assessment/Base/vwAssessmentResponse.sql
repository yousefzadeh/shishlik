{{ config(materialized="view") }}
{# Status
    [Description("Published")]
        TOBECOMPLETED = 1,
        [Description("In Progress")]
        INPROGRESS = 2,
        [Description("Completed")]
        SUBMITTED = 3,
        [Description("Closed")]
        CLOSED = 4,
        [Description("UnSent")]
        UNSENT = 100
 #}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [Status],
            case
                [Status]
                when 1
                then 'Published'
                when 2
                then 'In Progress'
                when 3
                then 'Completed'
                when 4
                then 'Closed'
                when 100
                then 'UnSent'
            end as StatusCode,
            cast([Version] as nvarchar(4000)) Version,
            [AssessmentId],
            [UserId],
            [SubmittedDate],
            cast([ExternalSummary] as nvarchar(4000)) ExternalSummary,
            cast([InternalSummary] as nvarchar(4000)) InternalSummary,
            cast([Name] as nvarchar(4000))[Name],
            [Score] RiskRatingWeightedScore,
            CONCAT([TenantId], [AssessmentId], [UserId], SubmittedDate) as PK
            , coalesce([LastModificationTime],[CreationTime]) as [UpdateTime]
        -- Note We need to determine the line item detail of this table.
        from {{ source("assessment_models", "AssessmentResponse") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "AssessmentResponse") }},
    {{ col_rename("TenantId", "AssessmentResponse") }},
    {{ col_rename("Status", "AssessmentResponse") }},
    {{ col_rename("StatusCode", "AssessmentResponse") }},
    {{ col_rename("Version", "AssessmentResponse") }},

    {{ col_rename("AssessmentId", "AssessmentResponse") }},
    {{ col_rename("UserId", "AssessmentResponse") }},
    {{ col_rename("SubmittedDate", "AssessmentResponse") }},
    {{ col_rename("ExternalSummary", "AssessmentResponse") }},

    {{ col_rename("InternalSummary", "AssessmentResponse") }},
    {{ col_rename("Name", "AssessmentResponse") }},
    {{ col_rename("RiskRatingWeightedScore", "AssessmentResponse") }},
    {{ col_rename("PK", "AssessmentResponse") }},
    {{ col_rename("UpdateTime", "AssessmentResponse") }}
from base

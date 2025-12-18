{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Name] as nvarchar(4000))[Name],
            cast([Institution] as nvarchar(4000)) Institution,
            cast([LimitationsExclusions] as nvarchar(4000)) LimitationsExclusions,
            cast([Employer] as nvarchar(4000)) Employer,
            [LastUpdated],
            [PositionId],
            cast([ResponsibilitySummary] as nvarchar(4000)) ResponsibilitySummary,
            [Status],
            [AccountableUserId],
            cast([ReviewCode] as nvarchar(4000)) ReviewCode,
            [TeamId],
            [SubmittedOn]
        from {{ source("assessment_models", "AccountabilityStatement") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AccountabilityStatement") }},
    {{ col_rename("TenantId", "AccountabilityStatement") }},
    {{ col_rename("Name", "AccountabilityStatement") }},
    {{ col_rename("Institution", "AccountabilityStatement") }},

    {{ col_rename("LimitationsExclusions", "AccountabilityStatement") }},
    {{ col_rename("Employer", "AccountabilityStatement") }},
    {{ col_rename("LastUpdated", "AccountabilityStatement") }},
    {{ col_rename("PositionId", "AccountabilityStatement") }},

    {{ col_rename("ResponsibilitySummary", "AccountabilityStatement") }},
    {{ col_rename("Status", "AccountabilityStatement") }},
    {{ col_rename("AccountableUserId", "AccountabilityStatement") }},
    {{ col_rename("ReviewCode", "AccountabilityStatement") }},

    {{ col_rename("TeamId", "AccountabilityStatement") }},
    {{ col_rename("SubmittedOn", "AccountabilityStatement") }}
from base

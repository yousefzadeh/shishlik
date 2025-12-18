{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Name] as nvarchar(4000))[Name],
            cast([Type] as nvarchar(4000))[Type],
            cast([Frequency] as nvarchar(4000)) Frequency,
            cast([Description] as nvarchar(4000)) Description,
            [NextAssessment],
            [TenantVendorId],
            cast([BusinessUnit] as nvarchar(4000)) BusinessUnit,
            cast([Criticality] as nvarchar(4000)) Criticality,
            cast([InherentRisk] as nvarchar(4000)) InherentRisk,
			cast(coalesce([LastModificationTime],[CreationTime]) as datetime2) as UpdateTime
        from {{ source("engagement_models", "Engagement") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Engagement") }},
    {{ col_rename("TenantId", "Engagement") }},
    {{ col_rename("Name", "Engagement") }},
    {{ col_rename("Type", "Engagement") }},

    {{ col_rename("Frequency", "Engagement") }},
    {{ col_rename("Description", "Engagement") }},
    {{ col_rename("NextAssessment", "Engagement") }},
    {{ col_rename("TenantVendorId", "Engagement") }},

    {{ col_rename("BusinessUnit", "Engagement") }},
    {{ col_rename("Criticality", "Engagement") }},
    {{ col_rename("InherentRisk", "Engagement") }},
    {{ col_rename("UpdateTime", "Engagement") }}
from base

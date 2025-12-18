{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Name] as nvarchar(4000))[Name],
            cast([Overview] as nvarchar(4000)) Overview,
            cast([ContactDetails] as nvarchar(4000)) ContactDetails,
            [Status]
        from {{ source("assessment_models", "CompliancePortalProfile") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "CompliancePortalProfile") }},
    {{ col_rename("TenantId", "CompliancePortalProfile") }},
    {{ col_rename("Name", "CompliancePortalProfile") }},
    {{ col_rename("Overview", "CompliancePortalProfile") }},

    {{ col_rename("ContactDetails", "CompliancePortalProfile") }},
    {{ col_rename("Status", "CompliancePortalProfile") }}
from base

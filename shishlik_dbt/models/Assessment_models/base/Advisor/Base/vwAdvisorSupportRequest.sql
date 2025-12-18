{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [TenantId],
            [ServiceProviderId],
            [AdvisorId],
            cast([EmailAddress] as nvarchar(4000)) EmailAddress,
            cast([Description] as nvarchar(4000)) Description,
            cast([PageUrl] as nvarchar(4000)) PageUrl,
            cast([PageName] as nvarchar(4000)) PageName
        from {{ source("assessment_models", "AdvisorSupportRequest") }}
    )

select
    {{ col_rename("Id", "AdvisorSupportRequest") }},
    {{ col_rename("CreationTime", "AdvisorSupportRequest") }},
    {{ col_rename("CreatorUserId", "AdvisorSupportRequest") }},
    {{ col_rename("TenantId", "AdvisorSupportRequest") }},

    {{ col_rename("ServiceProviderId", "AdvisorSupportRequest") }},
    {{ col_rename("AdvisorId", "AdvisorSupportRequest") }},
    {{ col_rename("EmailAddress", "AdvisorSupportRequest") }},
    {{ col_rename("Description", "AdvisorSupportRequest") }},

    {{ col_rename("PageUrl", "AdvisorSupportRequest") }},
    {{ col_rename("PageName", "AdvisorSupportRequest") }}
from base

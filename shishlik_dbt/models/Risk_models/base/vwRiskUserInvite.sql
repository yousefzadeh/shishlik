{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [TenantId],
            cast([Message] as nvarchar(4000)) Message,
            [UserId],
            [RiskId],
            [OrganizationUnitId]
        from {{ source("risk_models", "RiskUserInvite") }}

    )

select
    {{ col_rename("Id", "RiskUserInvite") }},
    {{ col_rename("CreationTime", "RiskUserInvite") }},
    {{ col_rename("CreatorUserId", "RiskUserInvite") }},
    {{ col_rename("TenantId", "RiskUserInvite") }},

    {{ col_rename("Message", "RiskUserInvite") }},
    {{ col_rename("UserId", "RiskUserInvite") }},
    {{ col_rename("RiskId", "RiskUserInvite") }},
    {{ col_rename("OrganizationUnitId", "RiskUserInvite") }}
from base

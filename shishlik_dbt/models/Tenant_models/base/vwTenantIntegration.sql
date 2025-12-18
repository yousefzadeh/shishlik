{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            cast([IntegrationId] as nvarchar(4000)) IntegrationId,
            [TenantId],
            cast([CustomData] as nvarchar(4000)) CustomData
        from {{ source("tenant_models", "TenantIntegration") }}
    )

select
    {{ col_rename("Id", "TenantIntegration") }},
    {{ col_rename("IntegrationId", "TenantIntegration") }},
    {{ col_rename("TenantId", "TenantIntegration") }},
    {{ col_rename("CustomData", "TenantIntegration") }}
from base

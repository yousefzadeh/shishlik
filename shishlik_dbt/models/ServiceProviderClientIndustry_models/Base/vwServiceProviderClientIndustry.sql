{{ config(materialized="view") }}

with
    base as (
        select [Id], [TenantId], [IndustryId]
        from {{ source("ServiceProviderClientIndustry_models", "ServiceProviderClientIndustry") }}
    )

select
    {{ col_rename("Id", "ServiceProviderClientIndustry") }},
    {{ col_rename("TenantId", "ServiceProviderClientIndustry") }},
    {{ col_rename("IndustryId", "ServiceProviderClientIndustry") }}
from base

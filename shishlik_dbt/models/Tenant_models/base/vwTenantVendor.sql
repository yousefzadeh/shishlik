{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId], -- Hub ID
            [VendorId], -- Spoke ID
            cast([Criticality] as nvarchar(4000)) Criticality,
            cast([Geography] as nvarchar(4000)) Geography,
            cast([Industry] as nvarchar(4000)) Industry,
            cast([InherentRisk] as nvarchar(4000)) InherentRisk,
            case
               when InherentRisk is null then 'No Risk Rating'
               when InherentRisk = 'VeryLow' then 'Very Low'
               when InherentRisk = 'VeryHigh' then 'Very High'
               else InherentRisk end InherentRiskCode,
            [IsArchived],
            cast([Website] as nvarchar(4000)) Website,
            cast([Name] as nvarchar(4000)) Name,
            ContactEmail,
            cast(coalesce(LastModificationTime,CreationTime) as datetime2) as UpdateTime
        from {{ source("tenant_models", "TenantVendor") }} tv {{ system_remove_IsDeleted() }} and IsArchived = 0
    )

select
    {{ col_rename("Id", "TenantVendor") }},
    {{ col_rename("TenantId", "TenantVendor") }},
    {{ col_rename("VendorId", "TenantVendor") }},
    {{ col_rename("Criticality", "TenantVendor") }},

    {{ col_rename("Geography", "TenantVendor") }},
    {{ col_rename("Industry", "TenantVendor") }},
    {{ col_rename("InherentRisk", "TenantVendor") }},
    {{ col_rename("InherentRiskCode", "TenantVendor") }},
    {{ col_rename("IsArchived", "TenantVendor") }},

    {{ col_rename("Website", "TenantVendor") }},
    {{ col_rename("Name", "TenantVendor") }},
    {{ col_rename("CreatorUserId", "TenantVendor") }},
    {{ col_rename("CreationTime", "TenantVendor") }},
    {{ col_rename("ContactEmail", "TenantVendor") }},
    {{ col_rename("UpdateTime", "TenantVendor") }}
from base

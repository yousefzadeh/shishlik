{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [Code], [DisplayName], [ParentId], [TenantId]
        	, cast(coalesce([LastModificationTime], [CreationTime]) as datetime2) as UpdateTime--date column addition for synapse incremental load
        from {{ source("assessment_models", "AbpOrganizationUnits") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AbpOrganizationUnits") }},
    {{ col_rename("Code", "AbpOrganizationUnits") }},
    {{ col_rename("DisplayName", "AbpOrganizationUnits") }},
    {{ col_rename("ParentId", "AbpOrganizationUnits") }},

    {{ col_rename("TenantId", "AbpOrganizationUnits") }},
    {{ col_rename("UpdateTime", "AbpOrganizationUnits") }}--date column addition for synapse incremental load
from base

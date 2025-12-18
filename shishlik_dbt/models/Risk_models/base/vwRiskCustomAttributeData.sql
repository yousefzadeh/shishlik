{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [RiskId],
            [ThirdPartyAttributesId],
            [TenantId],
            [OrganizationUnitId],
            [ThirdPartyControlId],
            [UserId]
        from {{ source("risk_models", "RiskCustomAttributeData") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskCustomAttributeData") }},
    {{ col_rename("CreationTime", "RiskCustomAttributeData") }},
    {{ col_rename("CreatorUserId", "RiskCustomAttributeData") }},
    {{ col_rename("LastModificationTime", "RiskCustomAttributeData") }},

    {{ col_rename("LastModifierUserId", "RiskCustomAttributeData") }},
    {{ col_rename("IsDeleted", "RiskCustomAttributeData") }},
    {{ col_rename("DeleterUserId", "RiskCustomAttributeData") }},
    {{ col_rename("DeletionTime", "RiskCustomAttributeData") }},

    {{ col_rename("RiskId", "RiskCustomAttributeData") }},
    {{ col_rename("ThirdPartyAttributesId", "RiskCustomAttributeData") }},
    {{ col_rename("TenantId", "RiskCustomAttributeData") }},
    {{ col_rename("OrganizationUnitId", "RiskCustomAttributeData") }},
    {{ col_rename("ThirdPartyControlId", "RiskCustomAttributeData") }},
    {{ col_rename("UserId", "RiskCustomAttributeData") }}
from base

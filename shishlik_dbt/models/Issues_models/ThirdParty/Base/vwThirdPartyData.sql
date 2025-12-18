{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, 
        TenantVendorId, 
        ThirdPartyAttributesId,
		cast(coalesce(LastModificationTime,CreationTime) as datetime2) as UpdateTime
        from {{ source("issue_models", "ThirdPartyData") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ThirdPartyData") }},
    {{ col_rename("CreationTime", "ThirdPartyData") }},
    {{ col_rename("CreatorUserId", "ThirdPartyData") }},
    {{ col_rename("LastModificationTime", "ThirdPartyData") }},

    {{ col_rename("LastModifierUserId", "ThirdPartyData") }},
    {{ col_rename("IsDeleted", "ThirdPartyData") }},
    {{ col_rename("DeleterUserId", "ThirdPartyData") }},
    {{ col_rename("DeletionTime", "ThirdPartyData") }},

    {{ col_rename("TenantVendorId", "ThirdPartyData") }},
    {{ col_rename("ThirdPartyAttributesId", "ThirdPartyData") }},
    {{ col_rename("UpdateTime", "ThirdPartyData") }}
from base

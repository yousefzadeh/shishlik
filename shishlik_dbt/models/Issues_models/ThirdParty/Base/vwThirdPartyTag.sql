{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, 
        [TagId], 
        [TenantVendorId], 
        [TenantId],
        cast(coalesce(LastModificationTime,CreationTime) as datetime2) as UpdateTime
        from {{ source("issue_models", "ThirdPartyTag") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ThirdPartyTag") }},
    {{ col_rename("CreationTime", "ThirdPartyTag") }},
    {{ col_rename("CreatorUserId", "ThirdPartyTag") }},
    {{ col_rename("LastModificationTime", "ThirdPartyTag") }},

    {{ col_rename("LastModifierUserId", "ThirdPartyTag") }},
    {{ col_rename("IsDeleted", "ThirdPartyTag") }},
    {{ col_rename("DeleterUserId", "ThirdPartyTag") }},
    {{ col_rename("DeletionTime", "ThirdPartyTag") }},

    {{ col_rename("TagId", "ThirdPartyTag") }},
    {{ col_rename("TenantVendorId", "ThirdPartyTag") }},
    {{ col_rename("TenantId", "ThirdPartyTag") }},
    {{ col_rename("UpdateTime", "ThirdPartyTag") }}
from base

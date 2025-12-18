with
    base as (
        select
            Id,
            TenantId,
            VendorGroupId,
            SpokeId,
            CreationTime,
            CreatorUserId,
            LastModificationTime,
            LastModifierUserId,
            DeleterUserId,
            DeletionTime,
            cast(coalesce(LastModificationTime,CreationTime) as datetime2) as UpdateTime
        from {{ source("tenant_models", "Rpt_TenantVendorGroup") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Rpt_TenantVendorGroup") }},
    {{ col_rename("TenantId", "Rpt_TenantVendorGroup") }},
    {{ col_rename("VendorGroupId", "Rpt_TenantVendorGroup") }},
    {{ col_rename("SpokeId", "Rpt_TenantVendorGroup") }},

    {{ col_rename("CreationTime", "Rpt_TenantVendorGroup") }},
    {{ col_rename("CreatorUserId", "Rpt_TenantVendorGroup") }},
    {{ col_rename("LastModificationTime", "Rpt_TenantVendorGroup") }},
    {{ col_rename("LastModifierUserId", "Rpt_TenantVendorGroup") }},

    {{ col_rename("DeleterUserId", "Rpt_TenantVendorGroup") }},
    {{ col_rename("DeletionTime", "Rpt_TenantVendorGroup") }},
    {{ col_rename("UpdateTime", "Rpt_TenantVendorGroup") }}
from base

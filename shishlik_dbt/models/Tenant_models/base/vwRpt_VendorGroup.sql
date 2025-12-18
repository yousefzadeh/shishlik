with
    base as (
        select
            Id,
            TenantId,
            VendorGroupId,
            PathLevel,
            Level1Group,
            Level2Group,
            Level3PlusGroups,
            CreationTime,
            CreatorUserId,
            LastModificationTime,
            LastModifierUserId,
            DeleterUserId,
            DeletionTime,
            cast(coalesce(LastModificationTime,CreationTime) as datetime2) as UpdateTime
        from {{ source("tenant_models", "Rpt_VendorGroup") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Rpt_VendorGroup") }},
    {{ col_rename("TenantId", "Rpt_VendorGroup") }},
    {{ col_rename("VendorGroupId", "Rpt_VendorGroup") }},
    {{ col_rename("PathLevel", "Rpt_VendorGroup") }},

    {{ col_rename("Level1Group", "Rpt_VendorGroup") }},
    {{ col_rename("Level2Group", "Rpt_VendorGroup") }},
    {{ col_rename("Level3PlusGroups", "Rpt_VendorGroup") }},
    {{ col_rename("CreationTime", "Rpt_VendorGroup") }},

    {{ col_rename("CreatorUserId", "Rpt_VendorGroup") }},
    {{ col_rename("LastModificationTime", "Rpt_VendorGroup") }},
    {{ col_rename("LastModifierUserId", "Rpt_VendorGroup") }},
    {{ col_rename("DeleterUserId", "Rpt_VendorGroup") }},
    {{ col_rename("DeletionTime", "Rpt_VendorGroup") }},
    {{ col_rename("UpdateTime", "Rpt_VendorGroup") }}
from base

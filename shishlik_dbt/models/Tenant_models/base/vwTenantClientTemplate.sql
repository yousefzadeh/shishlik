with
    base as (
        select
            Id,
            ClientTenantId,
            ClientTemplateTenantId,
            TenantId,
            CreationTime,
            CreatorUserId,
            LastModificationTime,
            LastModifierUserId,
            IsDeleted,
            DeleterUserId,
            DeletionTime
        from {{ source("tenant_models","TenantClientTemplate") }}
        where IsDeleted = 0
    ),
    final as (
        select
            {{ col_rename("Id", "TenantClientTemplate") }},
            {{ col_rename("ClientTenantId", "TenantClientTemplate") }},
            {{ col_rename("ClientTemplateTenantId", "TenantClientTemplate") }},
            {{ col_rename("TenantId", "TenantClientTemplate") }},
            {{ col_rename("CreationTime", "TenantClientTemplate") }},
            {{ col_rename("CreatorUserId", "TenantClientTemplate") }},
            {{ col_rename("LastModificationTime", "TenantClientTemplate") }},
            {{ col_rename("LastModifierUserId", "TenantClientTemplate") }},
            {{ col_rename("IsDeleted", "TenantClientTemplate") }},
            {{ col_rename("DeleterUserId", "TenantClientTemplate") }},
            {{ col_rename("DeletionTime", "TenantClientTemplate") }}
        from base
    )
select *
from final

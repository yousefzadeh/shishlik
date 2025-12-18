{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [AuthorityId],
            [PublishedById],
            [PublishedDate],
            [ArchivedDate],
            [IsArchived],
            coalesce(LastModificationTime, CreationTime) as UpdateTime
        from {{ source("tenant_models", "TenantAuthority") }} {{ system_remove_IsDeleted() }}
    )

select distinct
    {{ col_rename("Id", "TenantAuthority") }},
    {{ col_rename("TenantId", "TenantAuthority") }},
    {{ col_rename("AuthorityId", "TenantAuthority") }},
    {{ col_rename("PublishedById", "TenantAuthority") }},

    {{ col_rename("PublishedDate", "TenantAuthority") }},
    {{ col_rename("ArchivedDate", "TenantAuthority") }},
    {{ col_rename("IsArchived", "TenantAuthority") }},
    {{ col_rename("UpdateTime", "TenantAuthority") }}
from base

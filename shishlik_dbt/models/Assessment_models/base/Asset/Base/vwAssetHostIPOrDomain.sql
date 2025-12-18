{{ config(materialized="view") }}
with
    base as (
        select
            Id,
            TenantId,
            HostIPOrDomain,
            [Type],
            HostIPRegisterItemId,
            DomainRegisterItemId,
            CreationTime,
            CreatorUserId,
            LastModificationTime,
            LastModifierUserId,
            DeleterUserId,
            DeletionTime
        from {{ source("assessment_models", "AssetHostIPOrDomain") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AssetHostIPOrDomain") }},
    {{ col_rename("TenantId", "AssetHostIPOrDomain") }},
    {{ col_rename("HostIPOrDomain", "AssetHostIPOrDomain") }},
    {{ col_rename("Type", "AssetHostIPOrDomain") }},

    {{ col_rename("HostIPRegisterItemId", "AssetHostIPOrDomain") }},
    {{ col_rename("DomainRegisterItemId", "AssetHostIPOrDomain") }},
    {{ col_rename("CreationTime", "AssetHostIPOrDomain") }},
    {{ col_rename("CreatorUserId", "AssetHostIPOrDomain") }},

    {{ col_rename("LastModificationTime", "AssetHostIPOrDomain") }},
    {{ col_rename("LastModifierUserId", "AssetHostIPOrDomain") }},
    {{ col_rename("DeleterUserId", "AssetHostIPOrDomain") }},
    {{ col_rename("DeletionTime", "AssetHostIPOrDomain") }}
from base

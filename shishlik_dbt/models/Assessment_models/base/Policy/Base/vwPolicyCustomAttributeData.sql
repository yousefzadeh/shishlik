{{ config(materialized="view") }}
with
    base as (
        select
            Id,
            TenantId,
            PolicyId,
            ThirdPartyAttributesId,
            CreationTime,
            CreatorUserId,
            LastModificationTime,
            LastModifierUserId,
            DeleterUserId,
            DeletionTime

        from {{ source("assessment_models", "PolicyCustomAttributeData") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "PolicyCustomAttributeData") }},
    {{ col_rename("TenantId", "PolicyCustomAttributeData") }},
    {{ col_rename("PolicyId", "PolicyCustomAttributeData") }},
    {{ col_rename("ThirdPartyAttributesId", "PolicyCustomAttributeData") }},

    {{ col_rename("CreationTime", "PolicyCustomAttributeData") }},
    {{ col_rename("CreatorUserId", "PolicyCustomAttributeData") }},
    {{ col_rename("LastModificationTime", "PolicyCustomAttributeData") }},
    {{ col_rename("LastModifierUserId", "PolicyCustomAttributeData") }},

    {{ col_rename("DeleterUserId", "PolicyCustomAttributeData") }},
    {{ col_rename("DeletionTime", "PolicyCustomAttributeData") }}
from base

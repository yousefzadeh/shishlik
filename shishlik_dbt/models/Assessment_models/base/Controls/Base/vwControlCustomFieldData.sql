with
    base as (
        select
        Id,
        ControlId,
        ThirdPartyControlId,
        ThirdPartyAttributeId,
        TextValue,
        DateValue,
        NumberValue,
        TenantId,
        CreationTime,
        CreatorUserId,
        LastModificationTime,
        LastModifierUserId
        from {{ source("assessment_models", "ControlCustomFieldData") }} {{ system_remove_IsDeleted() }}
    )
select
    {{ col_rename("Id", "ControlCustomFieldData") }},
    {{ col_rename("ControlId", "ControlCustomFieldData") }},
    {{ col_rename("ThirdPartyControlId", "ControlCustomFieldData") }},
    {{ col_rename("ThirdPartyAttributeId", "ControlCustomFieldData") }},
    {{ col_rename("TextValue", "ControlCustomFieldData") }},
    {{ col_rename("DateValue", "ControlCustomFieldData") }},
    {{ col_rename("NumberValue", "ControlCustomFieldData") }},
    {{ col_rename("TenantId", "ControlCustomFieldData") }},
    {{ col_rename("CreationTime", "ControlCustomFieldData") }},
    {{ col_rename("CreatorUserId", "ControlCustomFieldData") }},
    {{ col_rename("LastModificationTime", "ControlCustomFieldData") }},
    {{ col_rename("LastModifierUserId", "ControlCustomFieldData") }}
from base


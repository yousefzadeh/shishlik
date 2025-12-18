{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [ThirdPartyDynamicFieldConfigurationId],
            [TenantId],
            cast([XAxisAttributeLabel] as nvarchar(4000)) XAxisAttributeLabel,
            cast([YAxisAttributeLabel] as nvarchar(4000)) YAxisAttributeLabel,
            cast([DynamicValue] as nvarchar(4000)) DynamicValue,
            cast([Description] as nvarchar(4000)) Description,
            [DynamicScoreValue],
            cast([DynamicColor] as nvarchar(4000)) DynamicColor,
            XAxisAttributeId,
            YAxisAttributeId
        from {{ source("issue_models", "ThirdPartyDynamicFieldData") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ThirdPartyDynamicFieldData") }},
    {{ col_rename("CreationTime", "ThirdPartyDynamicFieldData") }},
    {{ col_rename("CreatorUserId", "ThirdPartyDynamicFieldData") }},
    {{ col_rename("LastModificationTime", "ThirdPartyDynamicFieldData") }},

    {{ col_rename("LastModifierUserId", "ThirdPartyDynamicFieldData") }},
    {{ col_rename("IsDeleted", "ThirdPartyDynamicFieldData") }},
    {{ col_rename("DeleterUserId", "ThirdPartyDynamicFieldData") }},
    {{ col_rename("DeletionTime", "ThirdPartyDynamicFieldData") }},

    {{ col_rename("ThirdPartyDynamicFieldConfigurationId", "ThirdPartyDynamicFieldData") }},
    {{ col_rename("TenantId", "ThirdPartyDynamicFieldData") }},
    {{ col_rename("XAxisAttributeLabel", "ThirdPartyDynamicFieldData") }},
    {{ col_rename("YAxisAttributeLabel", "ThirdPartyDynamicFieldData") }},

    {{ col_rename("DynamicValue", "ThirdPartyDynamicFieldData") }},
    {{ col_rename("Description", "ThirdPartyDynamicFieldData") }},
    {{ col_rename("DynamicScoreValue", "ThirdPartyDynamicFieldData") }},
    {{ col_rename("DynamicColor", "ThirdPartyDynamicFieldData") }},

    {{ col_rename("XAxisAttributeId", "ThirdPartyDynamicFieldData") }},
    {{ col_rename("YAxisAttributeId", "ThirdPartyDynamicFieldData") }}
from base

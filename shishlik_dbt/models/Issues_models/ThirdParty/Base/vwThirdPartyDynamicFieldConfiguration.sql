{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [ThirdPartyControlId],
            cast([XAxisThirdPartyControlName] as nvarchar(4000)) XAxisThirdPartyControlName,
            cast([YAxisThirdPartyControlName] as nvarchar(4000)) YAxisThirdPartyControlName,
            XAxisThirdPartyControlId,
            YAxisThirdPartyControlId
        from {{ source("issue_models", "ThirdPartyDynamicFieldConfiguration") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ThirdPartyDynamicFieldConfiguration") }},
    {{ col_rename("CreationTime", "ThirdPartyDynamicFieldConfiguration") }},
    {{ col_rename("CreatorUserId", "ThirdPartyDynamicFieldConfiguration") }},
    {{ col_rename("LastModificationTime", "ThirdPartyDynamicFieldConfiguration") }},

    {{ col_rename("LastModifierUserId", "ThirdPartyDynamicFieldConfiguration") }},
    {{ col_rename("IsDeleted", "ThirdPartyDynamicFieldConfiguration") }},
    {{ col_rename("DeleterUserId", "ThirdPartyDynamicFieldConfiguration") }},
    {{ col_rename("DeletionTime", "ThirdPartyDynamicFieldConfiguration") }},

    {{ col_rename("TenantId", "ThirdPartyDynamicFieldConfiguration") }},
    {{ col_rename("ThirdPartyControlId", "ThirdPartyDynamicFieldConfiguration") }},
    {{ col_rename("XAxisThirdPartyControlName", "ThirdPartyDynamicFieldConfiguration") }},
    {{ col_rename("YAxisThirdPartyControlName", "ThirdPartyDynamicFieldConfiguration") }},

    {{ col_rename("XAxisThirdPartyControlId", "ThirdPartyDynamicFieldConfiguration") }},
    {{ col_rename("YAxisThirdPartyControlId", "ThirdPartyDynamicFieldConfiguration") }}
from base

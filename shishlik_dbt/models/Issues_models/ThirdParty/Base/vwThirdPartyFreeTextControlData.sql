{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantVendorId],
            [ThirdPartyControlId],
            cast([TextData] as nvarchar(4000)) TextData,
            [CustomDateValue],
            [NumberValue],
			cast(coalesce(LastModificationTime,CreationTime) as datetime2) as UpdateTime
        from {{ source("issue_models", "ThirdPartyFreeTextControlData") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ThirdPartyFreeTextControlData") }},
    {{ col_rename("CreationTime", "ThirdPartyFreeTextControlData") }},
    {{ col_rename("CreatorUserId", "ThirdPartyFreeTextControlData") }},
    {{ col_rename("LastModificationTime", "ThirdPartyFreeTextControlData") }},

    {{ col_rename("LastModifierUserId", "ThirdPartyFreeTextControlData") }},
    {{ col_rename("IsDeleted", "ThirdPartyFreeTextControlData") }},
    {{ col_rename("DeleterUserId", "ThirdPartyFreeTextControlData") }},
    {{ col_rename("DeletionTime", "ThirdPartyFreeTextControlData") }},

    {{ col_rename("TenantVendorId", "ThirdPartyFreeTextControlData") }},
    {{ col_rename("ThirdPartyControlId", "ThirdPartyFreeTextControlData") }},
    {{ col_rename("TextData", "ThirdPartyFreeTextControlData") }},
    {{ col_rename("CustomDateValue", "ThirdPartyFreeTextControlData") }},
    {{ col_rename("NumberValue", "ThirdPartyFreeTextControlData") }},
    {{ col_rename("UpdateTime", "ThirdPartyFreeTextControlData") }}
from base

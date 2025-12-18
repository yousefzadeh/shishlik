{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [RegisterRecordId],
            [ThirdPartyControlId],
            cast([TextData] as nvarchar(4000)) TextData,
            [TenantId],
            [CustomDateValue],
            [NumberValue]
        from {{ source("register_models", "RegisterRecordThirdPartyControlFreeText") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RegisterRecordThirdPartyControlFreeText") }},
    {{ col_rename("RegisterRecordId", "RegisterRecordThirdPartyControlFreeText") }},
    {{ col_rename("ThirdPartyControlId", "RegisterRecordThirdPartyControlFreeText") }},
    {{ col_rename("TextData", "RegisterRecordThirdPartyControlFreeText") }},

    {{ col_rename("TenantId", "RegisterRecordThirdPartyControlFreeText") }},
    {{ col_rename("CustomDateValue", "RegisterRecordThirdPartyControlFreeText") }},
    {{ col_rename("NumberValue", "RegisterRecordThirdPartyControlFreeText") }}
from base

{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [RegisterRecordId], [ThirdPartyAttributesId]
        from {{ source("register_models", "RegisterRecordCustomAttributesData") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RegisterRecordCustomAttributesData") }},
    {{ col_rename("TenantId", "RegisterRecordCustomAttributesData") }},
    {{ col_rename("RegisterRecordId", "RegisterRecordCustomAttributesData") }},
    {{ col_rename("ThirdPartyAttributesId", "RegisterRecordCustomAttributesData") }}
from base

{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [Signed], [DateSigned], [UserId], [AccountabilityStatementId], [TenantId]
        from {{ source("signature_models", "Signature") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Signature") }},
    {{ col_rename("Signed", "Signature") }},
    {{ col_rename("DateSigned", "Signature") }},
    {{ col_rename("UserId", "Signature") }},

    {{ col_rename("AccountabilityStatementId", "Signature") }},
    {{ col_rename("TenantId", "Signature") }}
from base

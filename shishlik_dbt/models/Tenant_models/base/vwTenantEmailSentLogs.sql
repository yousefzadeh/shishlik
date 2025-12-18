{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [IsEmailSent], [EmailSentDate]
        from {{ source("tenant_models", "TenantEmailSentLogs") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "TenantEmailSentLogs") }},
    {{ col_rename("TenantId", "TenantEmailSentLogs") }},
    {{ col_rename("IsEmailSent", "TenantEmailSentLogs") }},
    {{ col_rename("EmailSentDate", "TenantEmailSentLogs") }}
from base

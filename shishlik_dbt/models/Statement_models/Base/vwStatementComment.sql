{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Comment] as nvarchar(4000))[Comment],
            [UserId],
            [StatementId],
            [RiskStatus]
        from {{ source("statement_models", "StatementComment") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "StatementComment") }},
    {{ col_rename("TenantId", "StatementComment") }},
    {{ col_rename("Comment", "StatementComment") }},
    {{ col_rename("UserId", "StatementComment") }},

    {{ col_rename("StatementId", "StatementComment") }},
    {{ col_rename("RiskStatus", "StatementComment") }}
from base

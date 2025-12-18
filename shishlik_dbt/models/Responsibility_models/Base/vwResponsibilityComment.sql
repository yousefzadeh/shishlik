{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Comment] as nvarchar(4000))[Comment],
            [UserId],
            [StatementId] ResponsibilityId,
            [RiskStatus]
        from {{ source("statement_models", "StatementComment") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ResponsibilityComment") }},
    {{ col_rename("TenantId", "ResponsibilityComment") }},
    {{ col_rename("Comment", "ResponsibilityComment") }},
    {{ col_rename("UserId", "ResponsibilityComment") }},

    {{ col_rename("ResponsibilityId", "ResponsibilityComment") }},
    {{ col_rename("RiskStatus", "ResponsibilityComment") }}
from base

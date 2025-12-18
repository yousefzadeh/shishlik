{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            cast([Reason] as nvarchar(4000)) Reason,
            [AccountabilityStatementId],
            [TenantId]
        from {{ source("assessment_models", "AccountabilityStatementRejectionReason") }}
    )

select
    {{ col_rename("Id", "AccountabilityStatementRejectionReason") }},
    {{ col_rename("CreationTime", "AccountabilityStatementRejectionReason") }},
    {{ col_rename("CreatorUserId", "AccountabilityStatementRejectionReason") }},
    {{ col_rename("Reason", "AccountabilityStatementRejectionReason") }},

    {{ col_rename("AccountabilityStatementId", "AccountabilityStatementRejectionReason") }},
    {{ col_rename("TenantId", "AccountabilityStatementRejectionReason") }}
from base

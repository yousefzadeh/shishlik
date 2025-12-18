{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            cast([OrgChartJsonData] as nvarchar(4000)) OrgChartJsonData,
            cast([StatementJsonData] as nvarchar(4000)) StatementJsonData,
            [TenantId],
            [AccountabilityStatementId]
        from {{ source("assessment_models", "AccountabilityStatementSnapshot") }}
    )

select
    {{ col_rename("Id", "AccountabilityStatementSnapshot") }},
    {{ col_rename("CreationTime", "AccountabilityStatementSnapshot") }},
    {{ col_rename("CreatorUserId", "AccountabilityStatementSnapshot") }},
    {{ col_rename("OrgChartJsonData", "AccountabilityStatementSnapshot") }},

    {{ col_rename("StatementJsonData", "AccountabilityStatementSnapshot") }},
    {{ col_rename("TenantId", "AccountabilityStatementSnapshot") }},
    {{ col_rename("AccountabilityStatementId", "AccountabilityStatementSnapshot") }}
from base

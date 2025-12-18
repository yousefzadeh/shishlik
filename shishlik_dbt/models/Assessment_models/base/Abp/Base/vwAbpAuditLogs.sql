{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [BrowserInfo],
            [ClientIpAddress],
            [ClientName],
            [CustomData],
            [Exception],
            [ExecutionDuration],
            [ExecutionTime],
            [ImpersonatorTenantId],
            [ImpersonatorUserId],
            [MethodName],
            [Parameters],
            [ServiceName],
            [TenantId],
            [UserId]
        from {{ source("assessment_models", "AbpAuditLogs") }}
    )

select
    {{ col_rename("Id", "AbpAuditLogs") }},
    {{ col_rename("BrowserInfo", "AbpAuditLogs") }},
    {{ col_rename("ClientIpAddress", "AbpAuditLogs") }},
    {{ col_rename("ClientName", "AbpAuditLogs") }},

    {{ col_rename("CustomData", "AbpAuditLogs") }},
    {{ col_rename("Exception", "AbpAuditLogs") }},
    {{ col_rename("ExecutionDuration", "AbpAuditLogs") }},
    {{ col_rename("ExecutionTime", "AbpAuditLogs") }},

    {{ col_rename("ImpersonatorTenantId", "AbpAuditLogs") }},
    {{ col_rename("ImpersonatorUserId", "AbpAuditLogs") }},
    {{ col_rename("MethodName", "AbpAuditLogs") }},
    {{ col_rename("Parameters", "AbpAuditLogs") }},

    {{ col_rename("ServiceName", "AbpAuditLogs") }},
    {{ col_rename("TenantId", "AbpAuditLogs") }},
    {{ col_rename("UserId", "AbpAuditLogs") }}
from base

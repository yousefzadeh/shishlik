{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            cast([Data] as nvarchar(4000)) Data,
            [DataTypeName],
            [EntityId],
            [EntityTypeAssemblyQualifiedName],
            [EntityTypeName],
            [NotificationName],
            [Severity],
            [TenantId]
        from {{ source("assessment_models", "AbpTenantNotifications") }}
    )

select
    {{ col_rename("Id", "AbpTenantNotifications") }},
    {{ col_rename("Data", "AbpTenantNotifications") }},
    {{ col_rename("DataTypeName", "AbpTenantNotifications") }},
    {{ col_rename("EntityId", "AbpTenantNotifications") }},

    {{ col_rename("EntityTypeAssemblyQualifiedName", "AbpTenantNotifications") }},
    {{ col_rename("EntityTypeName", "AbpTenantNotifications") }},
    {{ col_rename("NotificationName", "AbpTenantNotifications") }},
    {{ col_rename("Severity", "AbpTenantNotifications") }},

    {{ col_rename("TenantId", "AbpTenantNotifications") }}
from base

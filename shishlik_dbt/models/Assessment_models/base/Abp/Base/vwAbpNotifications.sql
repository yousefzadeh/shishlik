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
            cast([ExcludedUserIds] as nvarchar(4000)) ExcludedUserIds,
            [NotificationName],
            [Severity],
            cast([TenantIds] as nvarchar(4000)) TenantIds,
            cast([UserIds] as nvarchar(4000)) UserIds
        from {{ source("assessment_models", "AbpNotifications") }}
    )

select
    {{ col_rename("Id", "AbpNotifications") }},
    {{ col_rename("Data", "AbpNotifications") }},
    {{ col_rename("DataTypeName", "AbpNotifications") }},
    {{ col_rename("EntityId", "AbpNotifications") }},

    {{ col_rename("EntityTypeAssemblyQualifiedName", "AbpNotifications") }},
    {{ col_rename("EntityTypeName", "AbpNotifications") }},
    {{ col_rename("ExcludedUserIds", "AbpNotifications") }},
    {{ col_rename("NotificationName", "AbpNotifications") }},

    {{ col_rename("Severity", "AbpNotifications") }},
    {{ col_rename("TenantIds", "AbpNotifications") }},
    {{ col_rename("UserIds", "AbpNotifications") }}
from base

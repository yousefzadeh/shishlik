{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [CreatorUserId],
            [EntityId],
            [EntityTypeAssemblyQualifiedName],
            [EntityTypeName],
            [NotificationName],
            [TenantId],
            [UserId]
        from {{ source("assessment_models", "AbpNotificationSubscriptions") }}
    )

select
    {{ col_rename("Id", "AbpNotificationSubscriptions") }},
    {{ col_rename("EntityId", "AbpNotificationSubscriptions") }},
    {{ col_rename("EntityTypeAssemblyQualifiedName", "AbpNotificationSubscriptions") }},
    {{ col_rename("EntityTypeName", "AbpNotificationSubscriptions") }},

    {{ col_rename("NotificationName", "AbpNotificationSubscriptions") }},
    {{ col_rename("TenantId", "AbpNotificationSubscriptions") }},
    {{ col_rename("UserId", "AbpNotificationSubscriptions") }}
from base

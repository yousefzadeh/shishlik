{{ config(materialized="view") }}
with
    base as (
        select [Id], [CreationTime], [State], [TenantId], [TenantNotificationId], [UserId]
        from {{ source("assessment_models", "AbpUserNotifications") }}
    )

select
    {{ col_rename("Id", "AbpUserNotifications") }},
    {{ col_rename("State", "AbpUserNotifications") }},
    {{ col_rename("TenantId", "AbpUserNotifications") }},
    {{ col_rename("TenantNotificationId", "AbpUserNotifications") }},

    {{ col_rename("UserId", "AbpUserNotifications") }}
from base

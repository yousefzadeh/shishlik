{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            [FriendProfilePictureId],
            cast([FriendTenancyName] as nvarchar(4000)) FriendTenancyName,
            [FriendTenantId],
            [FriendUserId],
            [FriendUserName],
            [State],
            [TenantId],
            [UserId]
        from {{ source("assessment_models", "AppFriendships") }}
    )

select
    {{ col_rename("Id", "AppFriendships") }},
    {{ col_rename("CreationTime", "AppFriendships") }},
    {{ col_rename("FriendProfilePictureId", "AppFriendships") }},
    {{ col_rename("FriendTenancyName", "AppFriendships") }},

    {{ col_rename("FriendTenantId", "AppFriendships") }},
    {{ col_rename("FriendUserId", "AppFriendships") }},
    {{ col_rename("FriendUserName", "AppFriendships") }},
    {{ col_rename("State", "AppFriendships") }},

    {{ col_rename("TenantId", "AppFriendships") }},
    {{ col_rename("UserId", "AppFriendships") }}
from base

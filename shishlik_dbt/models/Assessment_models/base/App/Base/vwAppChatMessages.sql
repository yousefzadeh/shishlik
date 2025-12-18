{{ config(materialized="view") }}
with
    base as (
        select
            [Id],
            [CreationTime],
            cast([Message] as nvarchar(4000)) Message,
            [ReadState],
            [Side],
            [TargetTenantId],
            [TargetUserId],
            [TenantId],
            [UserId],
            [SharedMessageId],
            [ReceiverReadState]
        from {{ source("assessment_models", "AppChatMessages") }}
    )

select
    {{ col_rename("Id", "AppChatMessages") }},
    {{ col_rename("CreationTime", "AppChatMessages") }},
    {{ col_rename("Message", "AppChatMessages") }},
    {{ col_rename("ReadState", "AppChatMessages") }},

    {{ col_rename("Side", "AppChatMessages") }},
    {{ col_rename("TargetTenantId", "AppChatMessages") }},
    {{ col_rename("TargetUserId", "AppChatMessages") }},
    {{ col_rename("TenantId", "AppChatMessages") }},

    {{ col_rename("UserId", "AppChatMessages") }},
    {{ col_rename("SharedMessageId", "AppChatMessages") }},
    {{ col_rename("ReceiverReadState", "AppChatMessages") }}
from base

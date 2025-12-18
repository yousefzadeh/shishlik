{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [SourceEntityId],
            [TargetEntityId],
            [RequestType],
            cast([RequestMessage] as nvarchar(4000)) RequestMessage,
            cast([RequestId] as nvarchar(4000)) RequestId,
            [RequestStatus],
            [RequestedTenantId],
            [RequestedUserId],
            cast([Arguments] as nvarchar(4000)) Arguments
        from {{ source("haileyrequest_models", "HaileyRequest") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "HaileyRequest") }},
    {{ col_rename("SourceEntityId", "HaileyRequest") }},
    {{ col_rename("TargetEntityId", "HaileyRequest") }},
    {{ col_rename("RequestType", "HaileyRequest") }},

    {{ col_rename("RequestMessage", "HaileyRequest") }},
    {{ col_rename("RequestId", "HaileyRequest") }},
    {{ col_rename("RequestStatus", "HaileyRequest") }},
    {{ col_rename("RequestedTenantId", "HaileyRequest") }},

    {{ col_rename("RequestedUserId", "HaileyRequest") }},
    {{ col_rename("Arguments", "HaileyRequest") }}
from base

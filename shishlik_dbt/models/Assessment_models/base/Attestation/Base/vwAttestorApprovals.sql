{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [AttestorId],
            [AttestationItemId],
            [Status],
            cast([Comment] as nvarchar(4000))[Comment],
            [TenantId]
        from {{ source("assessment_models", "AttestorApprovals") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AttestorApprovals") }},
    {{ col_rename("AttestorId", "AttestorApprovals") }},
    {{ col_rename("AttestationItemId", "AttestorApprovals") }},
    {{ col_rename("Status", "AttestorApprovals") }},

    {{ col_rename("Comment", "AttestorApprovals") }},
    {{ col_rename("TenantId", "AttestorApprovals") }}
from base

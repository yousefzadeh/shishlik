{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [AttestationId],
            [UserId],
            [Status],
            case
                when [Status] = 1
                then 'Not Yet Started '
                when [Status] = 2
                then 'In Progress'
                when [Status] = 3
                then 'Completed'
                when [Status] = 4
                then 'Closed Before Completion'
                else 'Undefined'
            end as [StatusCode],
            [TenantId],
            [CompletionDate],
            [IsNewUser]
        from {{ source("assessment_models", "AttestationAttestors") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AttestationAttestors") }},
    {{ col_rename("CreationTime", "AttestationAttestors") }},
    {{ col_rename("LastModificationTime", "AttestationAttestors") }},
    {{ col_rename("AttestationId", "AttestationAttestors") }},
    {{ col_rename("UserId", "AttestationAttestors") }},
    {{ col_rename("Status", "AttestationAttestors") }},
    {{ col_rename("StatusCode", "AttestationAttestors") }},

    {{ col_rename("TenantId", "AttestationAttestors") }},
    {{ col_rename("CompletionDate", "AttestationAttestors") }},
    {{ col_rename("IsNewUser", "AttestationAttestors") }}
from base

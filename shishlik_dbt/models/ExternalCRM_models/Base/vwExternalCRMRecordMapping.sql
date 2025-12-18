{{ config(materialized="view") }}

with
    base as (
        select
            [Id],
            [ExternalCRM],
            cast([ExternalCRMId] as nvarchar(4000)) ExternalCRMId,
            cast([EmailAddress] as nvarchar(4000)) EmailAddress,
            [UserId],
            [TenantId],
            [CRMObjectType]
        from {{ source("externalCRM_models", "ExternalCRMRecordMapping") }}
    )

select
    {{ col_rename("Id", "ExternalCRMRecordMapping") }},
    {{ col_rename("ExternalCRM", "ExternalCRMRecordMapping") }},
    {{ col_rename("ExternalCRMId", "ExternalCRMRecordMapping") }},
    {{ col_rename("EmailAddress", "ExternalCRMRecordMapping") }},

    {{ col_rename("UserId", "ExternalCRMRecordMapping") }},
    {{ col_rename("TenantId", "ExternalCRMRecordMapping") }},
    {{ col_rename("CRMObjectType", "ExternalCRMRecordMapping") }}
from base

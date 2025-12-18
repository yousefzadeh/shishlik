{{ config(materialized="view") }}

with
    base as (
        select [Id], [LastSyncUtc], [ExternalCRM] from {{ source("externalCRM_models", "ExternalCRMSyncTracker") }}
    )

select
    {{ col_rename("Id", "ExternalCRMSyncTracker") }},
    {{ col_rename("LastSyncUtc", "ExternalCRMSyncTracker") }},
    {{ col_rename("ExternalCRM", "ExternalCRMSyncTracker") }}
from base

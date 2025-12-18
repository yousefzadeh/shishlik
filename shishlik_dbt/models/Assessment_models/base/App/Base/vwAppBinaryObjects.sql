{{ config(materialized="view") }}
with base as (select [Id], [Bytes], [TenantId] from {{ source("assessment_models", "AppBinaryObjects") }})

select
    {{ col_rename("ID", "AppBinaryObjects") }},
    {{ col_rename("Bytes", "AppBinaryObjects") }},
    {{ col_rename("TenantId", "AppBinaryObjects") }}
from base

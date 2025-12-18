{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [TagId], [MetricId]
        from {{ source("metric_models", "MetricTag") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "MetricTag") }},
    {{ col_rename("TenantId", "MetricTag") }},
    {{ col_rename("TagId", "MetricTag") }},
    {{ col_rename("MetricId", "MetricTag") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [MetricId], [UserId], [OrganizationUnitId]
        from {{ source("metric_models", "MetricOwner") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "MetricOwner") }},
    {{ col_rename("CreationTime", "MetricOwner") }},
    {{ col_rename("LastModificationTime", "MetricOwner") }},
    {{ col_rename("TenantId", "MetricOwner") }},
    {{ col_rename("MetricId", "MetricOwner") }},
    {{ col_rename("UserId", "MetricOwner") }},

    {{ col_rename("OrganizationUnitId", "MetricOwner") }}
from base

{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Feature] as nvarchar(4000)) Feature,
            cast([Value] as nvarchar(4000)) Value,
            [MonthlyPrice],
            cast([FeaturePlanName] as nvarchar(4000)) FeaturePlanName,
            [ServiceProviderId],
            [EditionId]
        from {{ source("assessment_models", "AddOnFeatures") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AddOnFeatures") }},
    {{ col_rename("Feature", "AddOnFeatures") }},
    {{ col_rename("Value", "AddOnFeatures") }},
    {{ col_rename("MonthlyPrice", "AddOnFeatures") }},

    {{ col_rename("FeaturePlanName", "AddOnFeatures") }},
    {{ col_rename("ServiceProviderId", "AddOnFeatures") }},
    {{ col_rename("EditionId", "AddOnFeatures") }}
from base

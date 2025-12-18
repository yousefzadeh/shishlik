{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [AssetId],
            [ThirdPartyControlId],
            cast([TextData] as nvarchar(4000)) TextData,
            [TenantId],
            [CustomDateValue]
        from {{ source("assessment_models", "AssetThirdPartyControlFreeText") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AssetThirdPartyControlFreeText") }},
    {{ col_rename("AssetId", "AssetThirdPartyControlFreeText") }},
    {{ col_rename("ThirdPartyControlId", "AssetThirdPartyControlFreeText") }},
    {{ col_rename("TextData", "AssetThirdPartyControlFreeText") }},

    {{ col_rename("TenantId", "AssetThirdPartyControlFreeText") }},
    {{ col_rename("CustomDateValue", "AssetThirdPartyControlFreeText") }}
from base

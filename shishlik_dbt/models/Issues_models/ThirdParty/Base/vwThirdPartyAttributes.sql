{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Name] as nvarchar(4000))[Name],
            -- ,cast([Label] as varchar(100)) [Label]
            [Label],
            cast([Value] as varchar(100))[Value],
            [ThirdPartyControlId],
            cast([Description] as nvarchar(4000)) Description,
            cast([Color] as nvarchar(4000)) Color,
            LabelVarchar,
			cast(coalesce(LastModificationTime,CreationTime) as datetime2) as UpdateTime
        from {{ source("issue_models", "ThirdPartyAttributes") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ThirdPartyAttributes") }},
    {{ col_rename("Name", "ThirdPartyAttributes") }},
    {{ col_rename("Label", "ThirdPartyAttributes") }},
    {{ col_rename("Value", "ThirdPartyAttributes") }},

    {{ col_rename("ThirdPartyControlId", "ThirdPartyAttributes") }},
    {{ col_rename("Description", "ThirdPartyAttributes") }},
    {{ col_rename("Color", "ThirdPartyAttributes") }},
    {{ col_rename("LabelVarchar", "ThirdPartyAttributes") }},
    {{ col_rename("UpdateTime", "ThirdPartyAttributes") }}
from
    base

    /*There are multiple attributes per control

SELECT ThirdPartyAttributes_ThirdPartyControlId

,COUNT(*)
  FROM [dev_naunghton_williams].[vwThirdPartyAttributes]
  GROUP BY ThirdPartyAttributes_ThirdPartyControlId
  ORDER BY COUNT(*) desc

  */
    

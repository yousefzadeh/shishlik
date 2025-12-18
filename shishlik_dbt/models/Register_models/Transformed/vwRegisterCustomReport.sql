with
    combine as (
        select *
        from {{ ref("vwAssetRegisterReport") }} arr

        union all

        select *
        from {{ ref("vwRegisterCustom") }} rc
    )

select *
from
    combine c

    -- where c.Tenant_Id = 3
    

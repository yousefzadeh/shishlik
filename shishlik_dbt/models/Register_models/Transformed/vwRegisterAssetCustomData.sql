with
    combine as (
        select *
        from {{ ref("vwAssetCustomData") }} acd

        union all

        select *
        from {{ ref("vwRegisterRecordCustomData") }} rrcd
    )

select *
from combine c

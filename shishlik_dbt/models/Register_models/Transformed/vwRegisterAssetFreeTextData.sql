with
    combine as (
        select *
        from {{ ref("vwAssetFreeTextData") }} acd

        union all

        select *
        from {{ ref("vwRegisterRecordFreeTextData") }} rrcd
    )

select *
from combine c

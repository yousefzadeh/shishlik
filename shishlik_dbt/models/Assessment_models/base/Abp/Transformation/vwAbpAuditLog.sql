with
    audit as (
        select *
        from {{ ref("vwAbpRoleManagement") }} varm

        union all

        select *
        from {{ ref("vwAbpGroupManagement") }} vagm

        union all

        select *
        from {{ ref("vwAbpUserManagement") }} vaum
    )

select *
from audit

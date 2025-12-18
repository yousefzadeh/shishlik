{{ config(materialized="view") }}
with
    auth as (select Authority_Id, Authority_Name, Authority_UpdateTime from {{ ref("vwAuthority") }}),
    zero as (
        select distinct
            0 Authority_Id,
            'Unassigned (Authority Provision)' Authority_Name,
            cast('2000-01-01 00:00:01.000' as datetime) Authority_UpdateTime
        from auth
        union all
        select distinct
            -1 Authority_Id,
            'Unassigned (ControlSet Control)' Authority_Name,
            cast('2000-01-01 00:00:01.000' as datetime) Authority_UpdateTime
        from auth
    ),
    final as (
        select *
        from auth
        union all
        select *
        from zero
    )
select *
from final

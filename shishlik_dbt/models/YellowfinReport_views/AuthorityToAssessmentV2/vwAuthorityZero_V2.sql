{{ config(materialized="view") }}
with
    auth as (select Id Authority_Id, NameVarchar Authority_Name from {{ source("assessment_models", "Authority") }}),
    zero as (
        select distinct 0 Authority_Id, 'Unassigned (Authority Provision)' Authority_Name
        from auth
        union all
        select distinct -1 Authority_Id, 'Unassigned (ControlSet Control)' Authority_Name
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

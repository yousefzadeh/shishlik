-- One row per responsibility with columns for comma separated list of assignees and
-- owners
with
    ron as (
        select distinct
            r.responsibility_id,
            coalesce(
                ron.responsibilityowner_name, 'Unassigned'
            ) responsibilityowner_name
        from {{ ref("vwResponsibility") }} r
        left join
            {{ ref("vwResponsibilityOwnerName") }} ron
            on r.responsibility_id = ron.responsibility_id
    ),
    ron_agg as (
        select
            ron.responsibility_id,
            string_agg(
                coalesce(ron.responsibilityowner_name, 'Unassigned'), ', '
            ) ownernamelist
        from ron
        group by ron.responsibility_id
    ),
    ran as (
        select distinct
            r.responsibility_id,
            coalesce(
                ran.responsibilityassignee_name, 'Unassigned'
            ) responsibilityassignee_name
        from {{ ref("vwResponsibility") }} r
        left join
            {{ ref("vwResponsibilityAssigneeName") }} ran
            on r.responsibility_id = ran.responsibility_id
    ),
    ran_agg as (
        select
            ran.responsibility_id,
            string_agg(
                coalesce(ran.responsibilityassignee_name, 'Unassigned'), ', '
            ) assigneenamelist
        from ran
        group by ran.responsibility_id
    )
select r.responsibility_id, ron_agg.ownernamelist, ran_agg.assigneenamelist
from {{ ref("vwResponsibility") }} r
left join ron_agg on r.responsibility_id = ron_agg.responsibility_id
left join ran_agg on r.responsibility_id = ran_agg.responsibility_id

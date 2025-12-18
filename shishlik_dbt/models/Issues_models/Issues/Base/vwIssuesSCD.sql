{{ config(materialized="view") }}

with
    issue_master as (select * from {{ source("issue_models", "Issues") }} i where RootIssueId is null),
    issue_versions as (select * from {{ source("issue_models", "Issues") }} i where RootIssueId is not null)

select
    T.MasterId,
    T.Id,
    T.Version,
    T.CreationTime,
    lead(T.CreationTime) over (partition by T.MasterId order by T.version) NextCreationTime
from
    (
        select im.id as MasterId, im.*
        from issue_master im

        union all

        select im.id as MasterId, iv.*
        from issue_versions iv
        left join issue_master im on iv.RootIssueId = im.Id
        where iv.RootIssueId is not null
    ) as T

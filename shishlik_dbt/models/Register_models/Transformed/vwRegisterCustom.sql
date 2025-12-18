-- Custom Register Report Query
with
    rtag as (
        select it.IssueTag_IssueId, STRING_AGG(CAST(t.Tags_Name as nvarchar(MAX)), ', ') as tags

        from {{ ref("vwIssueTag") }} it
        join {{ ref("vwTags") }} t on t.Tags_Id = it.IssueTag_TagId
        group by it.IssueTag_IssueId
    ),
    rname as (
        select
            io.IssueOwner_IssueId,
            STRING_AGG(CAST(au.AbpUsers_FullName as nvarchar(MAX)), ', ') as FullName

        from {{ ref("vwIssueOwner") }} io
        join {{ ref("vwAbpUser") }} au on au.AbpUsers_Id = io.IssueOwner_UserId
        group by io.IssueOwner_IssueId
    )

select distinct
    -- r.Register_Id
    rr.RegisterRecord_TenantId Tenant_Id,
    rr.RegisterRecord_Id Id,
    r.Register_RegisterName Register,
    -- , rr.RegisterRecord_RegisterId
    rr.RegisterRecord_Name Name,
    rr.RegisterRecord_Description Description,
    rname.FullName Owner,
    au.AbpUsers_FullName Filter_Owner,
    rtag.Tags Tags,
    t.Tags_Name Filter_Tag,
    'N/A' Type,
    'N/A' Linked_Teams,
    'N/A' Filter_Linked_Teams
from {{ ref("vwRegister") }} r
join {{ ref("vwRegisterRecord") }} rr on rr.RegisterRecord_RegisterId = r.Register_Id
left join {{ ref("vwIssueTag") }} rrt on rrt.IssueTag_IssueId = rr.RegisterRecord_Id
left outer join {{ ref("vwTags") }} t on t.Tags_Id = rrt.IssueTag_TagId
left join {{ ref("vwIssueOwner") }} rro on rro.IssueOwner_IssueId = rr.RegisterRecord_Id
left outer join {{ ref("vwAbpUser") }} au on au.AbpUsers_Id = rro.IssueOwner_UserId
left outer join rtag on rtag.IssueTag_IssueId = rr.RegisterRecord_Id
left outer join
    rname on rname.IssueOwner_IssueId = rr.RegisterRecord_Id

    -- order by Name
    

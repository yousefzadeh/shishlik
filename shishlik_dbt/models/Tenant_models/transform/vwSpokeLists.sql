
-- Spoke Tags List - Tags table (if specific to a module then Tags table of that module like AssessmentTags, IssueTags tables etc.)
-- Spoke Groups List - Logged in with hub -> TenantVendor table -> TenantVendorGroup table (associated groups for spokes) -> VendorGroup table (group details)
-- Spoke Advisors List - Logged in with Hub -> TenantVendor  -> AbpTenants -> AbpUsers -> AbpUserRoles -> AbpRoles (get data from AbpUsers table with Advisor role)

with tag as (
    -- Tags by Spoke
    select distinct
    Tags_TenantId Hub_Id,         
    TenantVendor_VendorId Spoke_Id,
    Tags_Name TagName
    from {{ ref("vwTags") }}
    join {{ ref("vwTenantVendor") }} tv on Tags_TenantId = tv.TenantVendor_TenantId
),
spoke_group as (
    -- Group Names by Spoke
    select distinct
    Rpt_TenantVendorGroup_TenantId Hub_ID,
    Rpt_TenantVendorGroup_SpokeID Spoke_ID,
    VendorGroup_Name   GroupName,
    VendorGroup_Level GroupLevel
    from {{ ref("vwRpt_TenantVendorGroup") }} tvg 
    join {{ ref("vwVendorGroup") }} vg on vg.VendorGroup_Id = tvg.Rpt_TenantVendorGroup_VendorGroupId
),
advisor as (
    -- Advisors by Spoke
    select distinct
    Hub_Id,
    Spoke_ID,
    AdvisorName
    from {{ ref("vwSpokeAdvisor") }} 
),
tag_list as (
    -- Concatenated Tags by Spoke
    select  
    Hub_Id,
    Spoke_Id,
    string_agg(cast(TagName as varchar(max)), ', ') TagList
    from tag 
    where Hub_Id != Spoke_Id
    group by Hub_Id, Spoke_Id
),
group_list as (
    -- Concatenated Group Names by Spoke
    select 
    Hub_Id,
    Spoke_Id,
    string_agg(cast(GroupName as varchar(max)), ', ') WITHIN GROUP ( ORDER BY GroupLevel, GroupName ) GroupList
    from spoke_group 
    where Hub_Id != Spoke_Id
    group by Hub_Id, Spoke_Id
),
advisor_list as (
    -- Concatenated Advisor Names by Spoke
    select 
    Hub_Id,
    Spoke_Id,
    string_agg(cast(AdvisorName as varchar(max)), ', ') AdvisorList 
    from advisor 
    where Hub_Id != Spoke_Id
    group by Hub_Id, Spoke_Id
),
final as (
	select 
	coalesce(g.Hub_Id, a.Hub_Id, t.Hub_Id) Hub_Id,
    coalesce(g.Spoke_Id, a.Spoke_Id, t.Spoke_Id) Spoke_Id,
    g.GroupList,
	a.AdvisorList,
	t.TagList
	from group_list g 
	full outer join advisor_list a 
	on g.Hub_Id = a.Hub_Id and g.Spoke_ID = a.Spoke_ID
	full outer join tag_list t 
	on g.Hub_Id = t.Hub_Id and g.Spoke_ID = t.Spoke_ID
    where coalesce(g.Spoke_Id, a.Spoke_Id, t.Spoke_Id) is not NULL
)
select *
from final 

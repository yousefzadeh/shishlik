-- Asset Register Report Query
with
    atag as (
        select a.Asset_Id, STRING_AGG(t.Tags_Name, ', ') as tags

        from {{ ref("vwAsset") }} a
        left outer join {{ ref("vwIssueTag") }} at on at.IssueTag_IssueId = a.Asset_Id
        left outer join {{ ref("vwTags") }} t on t.Tags_Id = at.IssueTag_TagId
        group by a.Asset_Id
    ),
    aname as (
        select a.Asset_Id, STRING_AGG(au.AbpUsers_FullName, ', ') as FullName

        from {{ ref("vwAsset") }} a
        left outer join {{ ref("vwIssueOwner") }} ao on ao.IssueOwner_IssueId = a.Asset_Id
        left outer join {{ ref("vwAbpUser") }} au on au.AbpUsers_Id = ao.IssueOwner_UserId
        group by a.Asset_Id
    ),
    ateam as (
        select a.Asset_Id, STRING_AGG(tv.TenantVendor_Name, ', ') as Team

        from {{ ref("vwAsset") }} a
        left outer join {{ ref("vwIssueThirdParty") }} atv on atv.IssueThirdParty_IssueId = a.Asset_Id
        left outer join {{ ref("vwTenantVendor") }} tv on tv.TenantVendor_Id = atv.IssueThirdParty_TenantVendorId
        group by a.Asset_Id
    )

select distinct
    a.Asset_TenantId Tenant_Id,
    a.Asset_Id Id,
    'Assets' Register,
    a.Asset_Title Name,
    a.Asset_Description Description,
    aname.FullName Owner,
    au.AbpUsers_FullName Filter_Owner,
    atag.tags Tags,
    t.Tags_Name Filter_Tag,
    tpa.ThirdPartyAttributes_LabelVarchar Type,
    ateam.Team Linked_Teams,
    tv.TenantVendor_Name Filter_Linked_Teams

from {{ ref("vwAsset") }} a
left outer join {{ ref("vwIssueTag") }} at on at.IssueTag_IssueId = a.Asset_Id
left outer join {{ ref("vwTags") }} t on t.Tags_Id = at.IssueTag_TagId
left outer join {{ ref("vwIssueOwner") }} ao on ao.IssueOwner_IssueId = a.Asset_Id
left outer join {{ ref("vwAbpUser") }} au on au.AbpUsers_Id = ao.IssueOwner_UserId
left outer join {{ ref("vwIssueThirdParty") }} atv on atv.IssueThirdParty_IssueId = a.Asset_Id
left outer join {{ ref("vwTenantVendor") }} tv on tv.TenantVendor_Id = atv.IssueThirdParty_TenantVendorId
left outer join atag on atag.Asset_Id = a.Asset_Id
left outer join aname on aname.Asset_Id = a.Asset_Id
left outer join ateam on ateam.Asset_Id = a.Asset_Id
left join {{ ref("vwIssueCustomAttributeData") }} icad on icad.IssueCustomAttributeData_IssueId = a.Asset_Id
left join {{ ref("vwThirdPartyAttributes") }} tpa on tpa.ThirdPartyAttributes_Id = IssueCustomAttributeData_ThirdPartyAttributesId
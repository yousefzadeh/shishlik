with base as (
select distinct 
tv.Uuid Union_Id,
tv.TenantId Tenant_Id,
abp.[Name] Tenant_Name,
'Third-Parties' Record_Type,
tv.Id Record_Id,
COALESCE(tv.LastModificationTime, tv.CreationTime) Record_Last_Modification_Date_Time,
cast(tv.IsDeleted as int) Is_Deleted,
cast(NULL as varchar(1)) Module_Reference_Id,
tv.[Name] Record_Name,
cast(NULL as varchar(1)) Record_Description,
--Record_Linked_Data,
cast(NULL as varchar(1)) Record_Due_Date_Time,
cast(NULL as varchar(1)) Record_Overdue,
cast(NULL as varchar(1)) Record_Subtype,
(select tvo.TenantVendorOwner_UserOrgId as [User_Id], tvo.TenantVendorOwner_Name as [Name]
from {{ ref("vwTenantVendorOwnerName") }} tvo
where tvo.TenantVendorOwner_TenantVendorId = tv.Id for json path) Assigned_Owners,
(select tvu.TenantVendorUser_UserOrgId as [User_Id], tvu.TenantVendorUser_Name as [Name]
from {{ ref("vwTenantVendorUserName") }} tvu
where tvu.TenantVendorUser_TenantVendorId = tv.Id for json path) [Access_Members],
(select distinct STRING_AGG(t.[Name],',')
from {{ source("hailey_union_models", "ThirdPartyTag") }} tpt
join {{ source("hailey_union_models", "Tags") }} t on t.Id = tpt.TagId and tpt.IsDeleted = 0 and t.IsDeleted = 0
where tpt.TenantVendorId = tv.Id) [Tags],
cast(NULL as varchar(1)) TenantVendorId,
cast(NULL as varchar(1)) Additional_Info

from {{ source("hailey_union_models", "TenantVendor") }} tv
join {{ source("hailey_union_models", "AbpTenants") }} abp
on abp.Id = tv.TenantId
where abp.IsDeleted = 0 and abp.IsActive = 1 
and tv.IsArchived = 0
)
, rcd_status as (select
tpd.TenantVendorId, tpa.LabelVarchar
from {{ source("hailey_union_models", "ThirdPartyData") }} tpd
left join {{ source("hailey_union_models", "ThirdPartyAttributes") }} tpa
on tpa.Id = tpd.ThirdPartyAttributesId
left join {{ source("hailey_union_models", "ThirdPartyControl") }} tpc
on tpc.Id = tpa.ThirdPartyControlId
where tpc.LabelVarchar = 'Stage')

select b.*, rs.LabelVarchar as Record_Status
from base b
left join rcd_status rs
on rs.TenantVendorId = b.Record_Id
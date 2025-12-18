select distinct
a.Uuid Union_Id,
a.TenantId Tenant_Id,
abp.[Name] Tenant_Name,
'Assets' Record_Type,
a.Id Record_Id,
COALESCE(a.LastModificationTime, a.CreationTime) Record_Last_Modification_Date_Time,
cast(a.IsDeleted as int) Is_Deleted,
cast(NULL as varchar(1)) Module_Reference_Id,
a.Title Record_Name,
a.[Description] Record_Description,
----Record_Linked_Data
cast(NULL as varchar(1)) Record_Status,
cast(NULL as varchar(1)) Record_Due_Date_Time,
cast(NULL as varchar(1)) Record_Overdue,
(select sl.[Name]
from {{ source("hailey_union_models", "StatusLists") }} sl
where sl.Id = a.TypeId) Record_Subtype,
(select ao.AssetOwner_UserOrgId as [User_Id], ao.AssetOwner_Name as [Name]
from {{ ref("vwAssetOwnerName") }} ao
where ao.AssetOwner_AssetId = a.Id for json path) Assigned_Owners,
(select aam.UserId as [User_Id], au.Surname+' '+au.Name as [Name]
from {{ source("hailey_union_models", "AssetAccessMember") }} aam
join {{ source("hailey_union_models", "AbpUsers") }} au on au.Id = aam.UserId
where aam.AssetId = a.Id for json path) [Access_Members],
(select distinct STRING_AGG(t.[Name],',')
from {{ source("hailey_union_models", "AssetTag") }} ast
join {{ source("hailey_union_models", "Tags") }} t on t.Id = ast.TagId and ast.IsDeleted = 0 and t.IsDeleted = 0
where ast.AssetId = a.Id) [Tags],
cast(NULL as varchar(1)) TenantVendorId,
cast(NULL as varchar(1)) Additional_Info

from {{ source("hailey_union_models", "Asset") }} a
join {{ source("hailey_union_models", "AbpTenants") }} abp
on abp.Id = a.TenantId
where abp.IsDeleted = 0 and abp.IsActive = 1
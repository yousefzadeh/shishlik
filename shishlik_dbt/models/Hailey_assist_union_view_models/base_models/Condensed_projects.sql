select distinct
p.Uuid Union_Id,
p.TenantId Tenant_Id,
abp.[Name] Tenant_Name,
'Projects' Record_Type,
p.Id Record_Id,
COALESCE(p.LastModificationTime, p.CreationTime) Record_Last_Modification_Date_Time,
cast(p.IsDeleted as int) Is_Deleted,
cast(null as varchar(1)) Module_Reference_Id,
p.[Name] Record_Name,
p.[Description] Record_Description,
case when p.[Status] = 0 then 'Open' when p.[Status] = 1 then 'Closed' end as Record_Status,
cast(NULL as varchar(1)) Record_Due_Date_Time,
cast(NULL as varchar(1)) Record_Overdue,
cast(null as varchar(1)) Record_Subtype,
( select au.Id as [User_Id], au.[Name]+' '+au.Surname as [Name]
from {{ source("hailey_union_models", "AbpUsers") }} au
where au.Id = p.OwnerId and au.TenantId = p.TenantId and au.IsDeleted =0 and au.IsActive = 1 for json path) as Assigned_Owners,
cast(null as varchar(1)) [Access_Members],
(select distinct STRING_AGG(t.[Name],', ')
from {{ source("hailey_union_models", "ProjectTag") }} pt
join {{ source("hailey_union_models", "Tags") }} t on t.Id = pt.TagId and pt.IsDeleted = 0 and t.IsDeleted = 0
where pt.ProjectId = p.Id) [Tags],
cast(NULL as varchar(1)) TenantVendorId,
cast(NULL as varchar(1)) Additional_Info

from {{ source("hailey_union_models", "Project") }} p
join {{ source("hailey_union_models", "AbpTenants") }} abp on abp.Id = p.TenantId
where abp.IsDeleted = 0 and abp.IsActive = 1
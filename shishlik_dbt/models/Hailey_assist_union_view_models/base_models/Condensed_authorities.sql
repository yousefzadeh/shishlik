with authorities as(
select distinct
a.Uuid Union_Id,
a.TenantId Tenant_Id,
abp.[Name] Tenant_Name,
'Compliance' Record_Type,
a.Id Record_Id,
COALESCE(a.LastModificationTime, a.CreationTime) Record_Last_Modification_Date_Time,
cast(a.IsDeleted as int) Is_Deleted,
cast(NULL as varchar(1)) Module_Reference_Id,
a.[Name] Record_Name,
a.[Description] Record_Description,
----Record_Linked_Data
case when a.[Status] = 1 then 'Edit' when a.[Status] = 2 then 'Published' else 'Undefined' end Record_Status,
cast(NULL as varchar(1)) Record_Due_Date_Time,
cast(NULL as varchar(1)) Record_Overdue,
a.[Type] Record_Subtype,
abp.[Name] Assigned_Owners,
cast(NULL as varchar(1)) [Access_Members],
cast(NULL as varchar(1)) [Tags],
cast(NULL as varchar(1)) TenantVendorId,
cast(NULL as varchar(1)) Additional_Info
from {{ source("hailey_union_models", "Authority") }} a
join {{ source("hailey_union_models", "AbpTenants") }} abp
on abp.Id = a.TenantId
where abp.IsDeleted = 0 and abp.IsActive = 1
and IsArchived = 0

union all

select distinct
a.Uuid Union_Id,
ta.TenantId Tenant_Id,
abp.[Name] Tenant_Name,
'Compliance' Module,
ta.AuthorityId Record_Id,
COALESCE(a.LastModificationTime, a.CreationTime) Record_Last_Modification_Date_Time,
ta.IsDeleted Is_Deleted,
cast(NULL as varchar(1)) Module_Reference_Id,
a.[Name] Record_Name,
a.[Description] Record_Description,
----Record_Linked_Data
case when a.[Status] = 1 then 'Edit' when a.[Status] = 2 then 'Published' else 'Undefined' end Record_Status,
cast(NULL as varchar(1)) Record_Due_Date_Time,
cast(NULL as varchar(1)) Record_Overdue,
a.[Type] Record_Subtype,
abp2.[Name] Assigned_Owners,
cast(NULL as varchar(1)) [Access_Members],
cast(NULL as varchar(1)) [Tags],
cast(NULL as varchar(1)) TenantVendorId,
cast(NULL as varchar(1)) Additional_Info

from {{ source("hailey_union_models", "TenantAuthority") }} ta
join {{ source("hailey_union_models", "Authority") }} a
on a.Id = ta.AuthorityId
join {{ source("hailey_union_models", "AbpTenants") }} abp
on abp.Id = ta.TenantId
left join {{ source("hailey_union_models", "AbpTenants") }} abp2
on abp2.Id = a.TenantId
where abp.IsDeleted = 0 and abp.IsActive = 1
and ta.IsArchived = 0
)
select * from authorities
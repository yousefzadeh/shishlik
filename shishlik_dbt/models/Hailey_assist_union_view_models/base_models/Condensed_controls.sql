with policies as(
select
p.Uuid Union_Id,
p.TenantId,
p.Id,
p.Name,
p.CreationTime,
p.RootPolicyId,
p.[Version],
case when lead(p.CreationTime) over (partition by coalesce(p.RootPolicyId,p.Id)  order by [Version]) is null then 1 else 0 end  IsCurrent
from {{ source("hailey_union_models", "Policy") }} p
where p.IsDeleted = 0
)
, ControlOwnersBase as (
select distinct co.Id ControlOwner_Id, co.TenantId ControlOwner_TenantId, co.ControlId ControlOwner_ControlId, 
cast(au.[Name]+' '+au.Surname as varchar) as ControlOwnerUsers, 
cast(aou.DisplayName as varchar) ControlOwnerOrgs
from {{ source("hailey_union_models", "ControlOwner") }} co
left join {{ source("hailey_union_models", "AbpUsers") }} au on au.Id = co.UserId and au.IsDeleted = 0
left join {{ source("hailey_union_models", "AbpOrganizationUnits") }} aou on aou.Id = co.OrganizationUnitId and aou.IsDeleted = 0
where co.IsDeleted = 0
)
, ctrl_owner as(
select owners.*
from
ControlOwnersBase cob
unpivot (ControlOwners for Owners in (ControlOwnerUsers, ControlOwnerOrgs)) as owners
)

select
c.Uuid Union_Id,
c.TenantId Tenant_Id,
abp.[Name] Tenant_Name,
'Controls' Record_Type,
cast(p.Id as varchar(12))+'/'+cast(c.Id as varchar(12)) Record_Id,
COALESCE(c.LastModificationTime, c.CreationTime) Record_Last_Modification_Date_Time,
case when p.IsCurrent = 0 then 1 else cast(c.IsDeleted as int) end Is_Deleted,
c.Reference Module_Reference_Id,
p.Name+' > '+c.[Name] Record_Name,
dbo.udf_StripHTML(c.Detail) Record_Description,
----Record_Linked_Data
cast(null as varchar(10)) Record_Status,
cast(NULL as varchar(1)) Record_Due_Date_Time,
cast(NULL as varchar(1)) Record_Overdue,
cast(null as varchar(10)) Record_Subtype,
(select co.ControlOwner_Id as [User_Id], co.ControlOwners as [Name]
from ctrl_owner co
where co.ControlOwner_ControlId = c.Id for json path) Assigned_Owners,
cast(null as varchar(10)) [Access_Members],
cast(null as varchar(10)) [Tags],
cast(NULL as varchar(1)) TenantVendorId,
-- Updated Additional_Info to include PolicyId in an array format
--JSON_QUERY('[{"Policy_Id": ' + CAST(pd.PolicyId AS VARCHAR) + ',"Policy_Name": "' + p.Name + '"}]') AS Additional_Info
 cast(NULL as varchar(1)) Additional_Info

from policies p
join {{ source("hailey_union_models", "PolicyDomain") }} pd
on pd.PolicyId = p.Id and pd.TenantId = p.TenantId and pd.IsDeleted = 0
join {{ source("hailey_union_models", "Controls") }} c
on c.PolicyDomainId = pd.Id and c.TenantId = pd.TenantId
join {{ source("hailey_union_models", "AbpTenants") }} abp
on abp.Id = c.TenantId
where abp.IsDeleted = 0 and abp.IsActive = 1
and c.IsDeleted = 0
select distinct
a.Uuid Union_Id,
a.TenantId Tenant_Id,
abp.[Name] Tenant_Name,
'Assessments' Record_Type,
a.Id Record_Id,
COALESCE(a.LastModificationTime, a.CreationTime) Record_Last_Modification_Date_Time,
cast(a.IsDeleted as int) Is_Deleted,
cast(NULL as varchar(1)) Module_Reference_Id,
a.[Name] Record_Name,
a.[Description] Record_Description,
case
when a.[Status] = 1 then 'Draft'
when a.[Status] = 2 then 'Approved'
when a.[Status] = 3 then 'Published'
when a.[Status] = 4 then 'Completed'
when a.[Status] = 5 then 'Closed'
when a.[Status] = 6 then 'Reviewed'
when a.[Status] = 7 then 'In Progress'
when a.[Status] = 8 then 'Cancelled'
else 'Undefined'
end as Record_Status,
a.DueDate Record_Due_Date_Time,
case
when a.[Status] in (4, 5, 6) then 'No'
when a.[Status] not in (4, 5, 6) and a.DueDate > getdate() then 'No'
when a.[Status] not in (4, 5, 6) and a.DueDate < getdate() then 'Yes'
else 'No'
end Record_Overdue,
case
when a.WorkFlowId = 1 then 'RBA' 
when a.WorkFlowId = 0 then 'QBA' 
else 'Undefined'
end Record_Subtype,

( select aof.Owner_Id as [User_Id], aof.OwnerName as [Name]
  from {{ ref("vwAssessmentOwnerFilter") }} aof
  where aof.AssessmentOwner_AssessmentId = a.Id for json path) as Assigned_Owners,
cast(NULL as varchar(1)) as [Access_Members],
( select STRING_AGG(innerquery.Assessment_TagName,',')
  FROM (select distinct t.Assessment_TagName
from {{ ref("vwAssessmentTag") }} t
where t.AssessmentTag_AssessmentId = a.Id and t.AssessmentTag_TenantId = a.TenantId) as innerquery)  as [Tags],
a.TenantVendorId,
cast(NULL as varchar(1)) Additional_Info

from {{ source("hailey_union_models", "Assessment") }} a
join {{ source("hailey_union_models", "AbpTenants") }} abp on abp.Id = a.TenantId

where a.IsTemplate = 0 and a.IsDeprecatedAssessmentVersion = 0 and a.Status != 8 and a.IsArchived = 0 and abp.IsDeleted = 0 and abp.IsActive = 1
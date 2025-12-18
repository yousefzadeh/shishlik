with base as (
select distinct
i.Uuid Union_Id,
i.TenantId Tenant_Id,
abp.[Name] Tenant_Name,
'Custom Registers' Record_Type,
i.Id Record_Id,
COALESCE(i.LastModificationTime, i.CreationTime) Record_Last_Modification_Date_Time,
cast(i.IsDeleted as int) Is_Deleted,
cast(i.EntityRegisterItemId as varchar(8)) Module_Reference_Id,
i.[Name] Record_Name,
i.[Description] Record_Description,
--Record_Linked_Data,
case when wfs.Name is null then 'Unassigned'
else wfs.Name end as Record_Status,
i.DueDate Record_Due_Date_Time,
case 
when i.Stage = 100 then 'No'
when i.Stage in (0, 1, 2) and i.DueDate > getdate() then 'No'
when i.Stage in (0, 1, 2) and getdate()>i.DueDate then 'Yes'
else 'No'
end as Record_Overdue,
(select distinct iof.Owner_Id as [User_Id], iof.OwnerName as [Name]
from {{ ref("vwIssueOwnerFilter") }} iof
where iof.IssueOwner_IssueId = i.Id for json path) Assigned_Owners,
(select distinct iuf.Member_Id as [User_Id], iuf.UserText as [Name]
from {{ ref("vwIssueUserFilter") }} iuf
where iuf.IssueUser_IssueId = i.Id for json path) [Access_Members],
(select distinct STRING_AGG(t.[Name],',')
from {{ source("hailey_union_models", "IssueTag") }} it
join {{ source("hailey_union_models", "Tags") }} t on t.Id = it.TagId and it.IsDeleted = 0 and t.IsDeleted = 0
where it.IssueId = i.Id) as [Tags],
itp.TenantVendorId,
cast(NULL as varchar(1)) Additional_Info

from {{ source("hailey_union_models", "EntityRegister") }} er
join {{ source("hailey_union_models", "Issues") }} i
on i.EntityRegisterId = er.Id
join {{ source("hailey_union_models", "AbpTenants") }} abp
on abp.Id = i.TenantId
left join {{ source("hailey_union_models", "WorkflowStage") }} wfs
on wfs.Id = i.WorkflowStageId
left join {{ source("hailey_union_models", "IssueThirdParty") }} itp
on itp.IssueId = i.Id and itp.IsDeleted = 0
where abp.IsDeleted = 0 and abp.IsActive = 1
and er.IsDeleted = 0 and er.EntityType = 4
and i.IsArchived = 0 and i.[Status] != 100
)
, [Type] as (select icad.IssueId, tpa.[Label]
from {{ source("hailey_union_models", "IssueCustomAttributeData") }} icad
inner join {{ source("hailey_union_models", "ThirdPartyAttributes") }} tpa
on icad.ThirdPartyAttributesId = tpa.Id
inner join {{ source("hailey_union_models", "ThirdPartyControl") }} tpc
on tpa.ThirdPartyControlId = tpc.Id
where tpc.EntityType = 6 and tpc.Label in ('Type')
)

select b.*, t.Label as Record_Subtype
from base b
left join [Type] t
on t.IssueId = b.Record_Id
select distinct
r.Uuid Union_Id,
r.TenantId Tenant_Id,
abp.[Name] Tenant_Name,
'Risks' Record_Type,
r.Id Record_Id,
COALESCE(r.LastModificationTime, r.CreationTime) Record_Last_Modification_Date_Time,
cast(r.IsDeleted as int) Is_Deleted,
cast(r.TenantEntityUniqueId as varchar) Module_Reference_Id,
r.[Name] Record_Name,
r.[Description] Record_Description,
wfs.[Name] Record_Status,
cast(NULL as varchar(1)) Record_Due_Date_Time,
cast(NULL as varchar(1)) Record_Overdue,
cast(null as varchar(1)) Record_Subtype,
( select rof.Owner_Id as [User_Id], rof.OwnerText as [Name]
from {{ ref("vwRiskOwnerFilter") }} rof
where rof.RiskOwner_RiskId = r.Id and rof.RiskOwner_TenantId = r.TenantId for json path) as Assigned_Owners,
( select ruf.AccessMember_Id as [User_Id], ruf.UserText as [Name]
from {{ ref("vwRiskUserFilter") }} ruf
where ruf.RiskUser_TenantId = r.TenantId and ruf.RiskUser_RiskId = r.Id for json path) as [Access_Members], 
( select STRING_AGG(innerquery.Name,',') from 
(select distinct t.[Name]
from dbo.RiskTag rt left join dbo.Tags t on t.Id = rt.TagId and t.TenantId = rt.TenantId and t.IsDeleted = 0 and rt.IsDeleted = 0
where rt.RiskId = r.Id and rt.TenantId = r.TenantId) as innerquery) as [Tags],
rtp.TenantVendorId,
( select tpa.LabelVarchar as [Risk_Rating]
from {{ source("hailey_union_models", "ThirdPartyAttributes") }} tpa
where tpa.Id = r.RiskRatingId and tpa.IsDeleted = 0 for json path) as Additional_Info

from {{ source("hailey_union_models", "Risk") }} r
join {{ source("hailey_union_models", "AbpTenants") }} abp on abp.Id = r.TenantId
join {{ source("hailey_union_models", "WorkflowStage") }} wfs on wfs.Id = r.WorkflowStageId and wfs.IsDeleted = 0
left join {{ source("hailey_union_models", "RiskThirdParty") }} rtp on r.Id = rtp.RiskId and rtp.IsDeleted = 0

where abp.IsDeleted = 0 and abp.IsActive = 1
and r.[Status] != 100
with auth as (
select
a.Uuid,
a.Id Authority_Id,
a.CreationTime Authority_CreationTime,
a.CreatorUserId Authority_CreatorUserId,
a.LastModificationTime Authority_LastModificationTime,
a.LastModifierUserId Authority_LastModifierUserId,
a.Name Authority_Name,
a.Type Authority_Type,
a.Description Authority_Decription,
a.LastUpdated Authority_LastUpdated,
a.Body Authority_Body,
a.AuthoritySector Authority_Sector,
a.JurisdictionId Authority_JurisdictionId,
j.Name Authority_Jurisdiction,
a.Url Authority_RefURL,
a.ArchivedDate Authority_ArchivedDate,
a.LastPublishedTime Authority_LastPublishedTime,
a.Status Authority_StatusCode,
case when a.Status = 1 then 'Draft' when a.Status = 2 then 'Published' else 'Undefined' end as Authority_Status,
a.TenantId TenantId,
a.CreatedFromAuthorityId Authority_CreatedFromAuthorityId

from {{ source("authority_ref_models", "Authority") }} a
left join {{ source("authority_ref_models", "Jurisdiction") }} j on j.Id = a.JurisdictionId and j.IsDeleted = 0
where a.IsDeleted = 0
and a.IsArchived = 0

union all

select
ta.Uuid,
ta.AuthorityId Authority_Id,
ta.CreationTime Authority_CreationTime,
ta.CreatorUserId Authority_CreatorUserId,
ta.LastModificationTime Authority_LastModificationTime,
ta.LastModifierUserId Authority_LastModifierUserId,
a.Name Authority_Name,
a.Type Authority_Type,
a.Description Authority_Decription,
a.LastUpdated Authority_LastUpdated,
a.Body Authority_Body,
a.AuthoritySector Authority_Sector,
a.JurisdictionId Authority_JurisdictionId,
j.Name Authority_Jurisdiction,
a.Url Authority_RefURL,
ta.ArchivedDate Authority_ArchivedDate,
a.LastPublishedTime Authority_LastPublishedTime,
a.Status Authority_StatusCode,
case when a.Status = 1 then 'Draft' when a.Status = 2 then 'Published' else 'Undefined' end as Authority_Status,
ta.TenantId TenantId,
a.CreatedFromAuthorityId Authority_CreatedFromAuthorityId

from {{ source("authority_ref_models", "TenantAuthority") }} ta
join {{ source("authority_ref_models", "Authority") }} a
left join {{ source("authority_ref_models", "Jurisdiction") }} j on j.Id = a.JurisdictionId and j.IsDeleted = 0
on ta.AuthorityId = a.Id
where ta.IsDeleted = 0
and ta.IsArchived = 0
)

select
TenantId,
t.Name TenantName,
auth.Uuid,
Authority_Id,
Authority_CreationTime,
Authority_CreatorUserId,
Authority_LastModificationTime,
Authority_LastModifierUserId,
Authority_Name,
Authority_Type,
Authority_Decription,
Authority_LastUpdated,
Authority_Body,
Authority_Sector,
Authority_JurisdictionId,
Authority_Jurisdiction,
Authority_RefURL,
Authority_ArchivedDate,
Authority_LastPublishedTime,
Authority_Status,
Authority_StatusCode,
Authority_CreatedFromAuthorityId

from auth
join AbpTenants t on t.id = auth.TenantId
where t.IsDeleted = 0 and t.IsActive = 1
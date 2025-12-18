with
    auth as (
        select
            Authority_Id,
            Authority_Name,
            Authority_Url,
            Authority_Type,
            Authority_Description,
            Authority_LastUpdated,
            Authority_FileName,
            Authority_Fileurl,
            Authority_IsUploadedToHailey,
            Authority_Body,
            Authority_AuthoritySector,
            Authority_JurisdictionId,
            Authority_ArchivedDate,
            Authority_IsArchived,
            Authority_LastPublishedTime,
            Authority_Status,
            Authority_StatusCode,
            Authority_TenantId Tenant_Id,
            COALESCE(Authority_CreatedFromAuthorityId, 0) Authority_CreatedFromAuthorityId,
            1 IsCreatedBy6Clicks
        from {{ ref("vwAuthority") }}
    ),
    tenant_auth as (
        select
            Authority_Id,
            Authority_Name,
            Authority_Url,
            Authority_Type,
            Authority_Description,
            Authority_LastUpdated,
            Authority_FileName,
            Authority_Fileurl,
            Authority_IsUploadedToHailey,
            Authority_Body,
            Authority_AuthoritySector,
            Authority_JurisdictionId,
            Authority_ArchivedDate,
            Authority_IsArchived,
            Authority_LastPublishedTime,
            Authority_Status,
            Authority_StatusCode,
            TenantAuthority_TenantId Tenant_Id,
            COALESCE(Authority_CreatedFromAuthorityId, 0) Authority_CreatedFromAuthorityId,
            0 IsCreatedBy6Clicks
        from {{ ref("vwAuthority") }} a
        join
            (
                select distinct TenantAuthority_TenantId, TenantAuthority_AuthorityId
                from {{ ref("vwTenantAuthority") }}
            ) ta
            on a.Authority_Id = ta.TenantAuthority_AuthorityId
    ),
    zero as (

        select
            0 Authority_Id,
            'Unassigned Authority' Authority_Name,
            NULL Authority_Url,
            NULL Authority_Type,
            'Unassigned Authority' Authority_Description,
            NULL Authority_LastUpdated,
            NULL Authority_FileName,
            NULL Authority_Fileurl,
            NULL Authority_IsUploadedToHailey,
            NULL Authority_Body,
            NULL Authority_AuthoritySector,
            0 Authority_JurisdictionId,
            NULL Authority_ArchivedDate,
            NULL Authority_IsArchived,
            NULL Authority_LastPublishedTime,
            NULL Authority_Status,
            NULL Authority_StatusCode,
            0 Tenant_Id,
            0 Authority_CreatedFromAuthorityId,
            1 IsCreatedBy6Clicks
    ),
    un as (
        select *
        from auth
        union all
        select *
        from tenant_auth
        union all
        select *
        from zero
    )

select distinct child.*, parent.Authority_Name Authority_CreatedFromAuthorityName
from un child
left join un parent on child.Authority_CreatedFromAuthorityId = parent.Authority_Id

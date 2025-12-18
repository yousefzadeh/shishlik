with
    base as (

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
            Tenant_Id,
            Authority_CreatedFromAuthorityId,
            IsCreatedBy6Clicks,
            AuthorityProvision_Id,
            AuthorityProvision_TenantId,
            AuthorityProvision_Name,
            AuthorityProvision_ReferenceId,
            AuthorityProvision_Description,
            AuthorityProvision_URL,
            AuthorityProvision_AuthorityId,
            AuthorityProvision_CustomDataJson,
            AuthorityProvision_Order
        from {{ ref("DimAuthority") }} a
        join {{ ref("vwAuthorityProvision") }} ap on a.Authority_id = ap.AuthorityProvision_AuthorityId
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
            1 IsCreatedBy6Clicks,
            0 AuthorityProvision_Id,
            0 AuthorityProvision_TenantId,
            NULL AuthorityProvision_Name,
            NULL AuthorityProvision_ReferenceId,
            NULL AuthorityProvision_Description,
            NULL AuthorityProvision_URL,
            0 AuthorityProvision_AuthorityId,
            NULL AuthorityProvision_CustomDataJson,
            NULL AuthorityProvision_Order

    ),
    un as (
        select *
        from base
        union all
        select *
        from zero
    )

select *
from un

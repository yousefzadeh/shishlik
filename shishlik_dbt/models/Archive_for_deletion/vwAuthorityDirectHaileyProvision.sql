-- Taken from vwAuthorityToAssessmentRelatedAuthorities.sql
-- Source Authority with all provisions
-- Target Authority with all provisions
with
    direct_auth as (select Tenant_Id, Authority_Id, Authority_Name from {{ ref("vwDirectAuthority") }}),
    related_ap as (
        select
            tapm.TenantAuthorityProvisionMapping_SourceAuthorityProvisionId Source_AuthorityProvisionId,
            tapm.TenantAuthorityProvisionMapping_TenantId mapping_TenantId,
            ap1.AuthorityProvision_TenantId Source_ap_TenantId,
            ap2.AuthorityProvision_TenantId Target_ap_TenantId,
            ap1.AuthorityProvision_AuthorityId Source_AuthorityId,
            ap1.AuthorityProvision_ReferenceId Source_ReferenceId,
            ap1.AuthorityProvision_Name Source_Name,
            tapm.TenantAuthorityProvisionMapping_TargetAuthorityProvisionId Target_AuthorityProvisionId,
            ap2.AuthorityProvision_AuthorityId Target_AuthorityId,
            ap2.AuthorityProvision_ReferenceId Target_ReferenceId,
            ap2.AuthorityProvision_Name Target_Name
        -- count(distinct tapm.TenantAuthorityProvisionMapping_SourceAuthorityProvisionId) source_ap_count
        from {{ ref("vwTenantAuthorityProvisionMapping") }} tapm
        join
            {{ ref("vwAuthorityProvision") }} ap1
            on tapm.TenantAuthorityProvisionMapping_SourceAuthorityProvisionid = ap1.AuthorityProvision_Id
        join
            {{ ref("vwAuthorityProvision") }} ap2
            on tapm.TenantAuthorityProvisionMapping_TargetAuthorityProvisionId = ap2.AuthorityProvision_Id
    ),
    related_auth_thru_ap as (
        -- Direct and Related Authority thru AuthorityProvision table Source
        select distinct
            mapping_TenantId,
            source_ap_TenantId,
            source_AuthorityId Authority_Id,
            target_ap_TenantId Related_TenantId,
            target_AuthorityId Related_AuthorityId
        from related_ap
    ),
    source_ap as (
        -- Direct Authority with mapping and ALL Provisions
        select
            ra.mapping_TenantId,
            ra.Authority_Id,  -- Direct authority with a Provision Mapping
            ra.Related_AuthorityId,  -- Related Authority thru AP mapping
            ap.AuthorityProvision_Id source_ap  -- ALL Provisions
        from related_auth_thru_ap ra
        join
            {{ ref("vwAuthorityProvision") }} ap
            on ap.AuthorityProvision_AuthorityId = ra.Authority_Id
            and ap.AuthorityProvision_TenantId = ra.source_ap_TenantId

    ),
    target_ap as (
        -- Related Authority and ALL Provisions
        select
            ra.mapping_TenantId,
            ra.source_ap_TenantId,
            ra.Authority_Id,
            ra.Related_TenantId,
            ra.Related_AuthorityId,
            ap.AuthorityProvision_Id target_ap
        from related_auth_thru_ap ra
        join
            {{ ref("vwAuthorityProvision") }} ap
            on ap.AuthorityProvision_AuthorityId = ra.Related_AuthorityId
            and ap.AuthorityProvision_TenantId = ra.Related_TenantId
    ),
    all_ap as (
        select mapping_TenantId Tenant_Id, Authority_Id, Related_AuthorityId, source_ap, NULL target_ap
        from source_ap

        union all

        select mapping_TenantId Tenant_Id, Authority_Id, Related_AuthorityId, NULL source_ap, target_ap
        from target_ap
    ),
    all_ap_long as (
        select
            mapping_TenantId Tenant_Id,
            Authority_Id,
            -- Related_AuthorityId,
            source_ap AuthorityProvision_Id,
            -- NULL target_ap
            'Direct' Related
        from source_ap

        union all

        select
            mapping_TenantId Tenant_Id,
            -- Authority_Id,
            Related_AuthorityId Authority_Id,
            -- NULL source_ap,
            target_ap AuthorityProvision_Id,
            'Hailey' Related
        from target_ap
    )
select Tenant_Id, Authority_Id, Related_AuthorityId, source_ap, target_ap
from all_ap

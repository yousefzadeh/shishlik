-- Equivalent query to auth_map left join ap_map
-- result: 
-- TenantId = Assessment_TenantId 
-- SourceAuthorityId = Assessment_AuthorityId (thru provision and control) 
-- SourceAuthorityProvisionId = ProvisionQuestion_AuthorityProvisionId (thru provision and control) 
--
-- TargetAuthorityId with 0 if NULL 
-- TargetAuthorityProvisionId with 0 if NULL 
-- TargetAuthority_Name 'Unassigned Target Authority' 
-- TargetAuthorityProvision_Name 'Unassigned Target Provision'
-- TargetAuthorityProvision_RefId 'Unassigned Target Provision Id'
--
with
    ap_map as (
        select
            tapm.TenantAuthorityProvisionMapping_Id,
            tapm.TenantAuthorityProvisionMapping_TenantId,
            tapm.TenantAuthorityProvisionMapping_SourceAuthorityProvisionId,
            source_ap.AuthorityProvision_AuthorityId TenantAuthorityProvisionMapping_SourceAuthorityId,
            tapm.TenantAuthorityProvisionMapping_TargetAuthorityProvisionId,
            target_ap.AuthorityProvision_AuthorityId TenantAuthorityProvisionMapping_TargetAuthorityId
        from {{ ref("vwTenantAuthorityProvisionMapping") }} tapm
        join
            {{ ref("vwAuthorityProvision") }} source_ap
            on TenantAuthorityProvisionMapping_SourceAuthorityProvisionId = source_ap.AuthorityProvision_Id
        join
            {{ ref("vwAuthorityProvision") }} target_ap
            on TenantAuthorityProvisionMapping_TargetAuthorityProvisionId = target_ap.AuthorityProvision_Id
    ),
    inner_join as (
        select
            auth_map.TenantAuthorityMapping_TenantId,
            auth_map.TenantAuthorityMapping_SourceAuthorityId,
            ap_map.TenantAuthorityProvisionMapping_SourceAuthorityProvisionId,
            auth_map.TenantAuthorityMapping_TargetAuthorityId,
            ap_map.TenantAuthorityProvisionMapping_TargetAuthorityProvisionId
        from {{ ref("vwTenantAuthorityMapping") }} auth_map
        join
            ap_map
            on TenantAuthorityMapping_TenantId = TenantAuthorityProvisionMapping_TenantId
            and TenantAuthorityMapping_SourceAuthorityId = TenantAuthorityProvisionMapping_SourceAuthorityId
            and TenantAuthorityMapping_TargetAuthorityId = TenantAuthorityProvisionMapping_TargetAuthorityId
    ),
    left_join as (
        select
            auth_map.TenantAuthorityMapping_TenantId,
            auth_map.TenantAuthorityMapping_SourceAuthorityId,
            0 TenantAuthorityProvisionMapping_SourceAuthorityProvisionId,
            auth_map.TenantAuthorityMapping_TargetAuthorityId,
            0 TenantAuthorityProvisionMapping_TargetAuthorityProvisionId
        from {{ ref("vwTenantAuthorityMapping") }} auth_map
        left join
            ap_map
            on TenantAuthorityMapping_TenantId = TenantAuthorityProvisionMapping_TenantId
            and TenantAuthorityMapping_SourceAuthorityId = TenantAuthorityProvisionMapping_SourceAuthorityId
            and TenantAuthorityMapping_TargetAuthorityId = TenantAuthorityProvisionMapping_TargetAuthorityId
        where ap_map.TenantAuthorityProvisionMapping_Id is NULL
    ),
    final as (
        select *
        from inner_join
        union all
        select *
        from left_join
    )
select *
from final

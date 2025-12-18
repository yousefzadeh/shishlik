with
    direct_auth as (select * from {{ ref("vwDirectAuthority") }}),
    all_ap as (select * from {{ ref("vwAuthorityDirectHaileyProvision") }} adhp),
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
        from {{ ref("vwTenantAuthorityProvisionMapping") }} tapm
        join
            {{ ref("vwAuthorityProvision") }} ap1
            on tapm.TenantAuthorityProvisionMapping_SourceAuthorityProvisionid = ap1.AuthorityProvision_Id
        join
            {{ ref("vwAuthorityProvision") }} ap2
            on tapm.TenantAuthorityProvisionMapping_TargetAuthorityProvisionId = ap2.AuthorityProvision_Id
    ),
    count_all as (
        select
            'All provisions' Scope,
            ra.Tenant_Id,
            ra.Authority_Id,
            ra.Related_AuthorityId,
            count(distinct source_ap) source_ap_count,
            count(distinct target_ap) target_ap_count
        from all_ap ra
        group by ra.Tenant_Id, ra.Authority_Id, ra.Related_AuthorityId
    ),
    count_related as (
        select
            'Related' scope,
            mapping_TenantId Tenant_Id,
            source_AuthorityId Authority_Id,
            target_AuthorityId Related_AuthorityId,
            count(distinct Source_AuthorityProvisionId) source_ap_count,
            count(distinct Target_AuthorityProvisionId) target_ap_count
        from related_ap rap
        group by mapping_TenantId, source_AuthorityId, target_AuthorityId
    ),
    un as (
        select *
        from
            (
                select *
                from count_all
                union all
                select *
                from count_related
            ) as T
    ),
    j as (
        select
            all_ap.Tenant_Id,
            all_ap.Authority_Id,
            all_ap.Related_AuthorityId,
            all_ap.source_ap_count all_source_ap_count,
            all_ap.target_ap_count all_target_ap_count,
            rel_ap.source_ap_count related_source_ap_count,
            rel_ap.target_ap_count related_target_ap_count,
            (rel_ap.source_ap_count * 100.0) / (all_ap.source_ap_count * 1.0) source_ap_count_percent,
            (rel_ap.target_ap_count * 100.0) / (all_ap.target_ap_count * 1.0) target_ap_count_percent
        from count_all all_ap
        join
            count_related rel_ap
            on all_ap.Tenant_Id = rel_ap.Tenant_Id
            and all_ap.Authority_Id = rel_ap.Authority_Id
            and all_ap.Related_AuthorityId = rel_ap.Related_AuthorityId
    )

select distinct
    d.Tenant_Id,
    d.Authority_Id,
    d.Authority_Name,
    j.Related_AuthorityId,
    ra.Authority_Name Related_AuthorityName,
    j.all_source_ap_count,
    j.all_target_ap_count,
    j.related_source_ap_count,
    j.related_target_ap_count,
    j.source_ap_count_percent,
    j.target_ap_count_percent
from direct_auth d
left join j on d.Tenant_Id = j.Tenant_Id and d.Authority_Id = j.Authority_Id
left join direct_auth ra on j.Related_AuthorityId = ra.Authority_Id

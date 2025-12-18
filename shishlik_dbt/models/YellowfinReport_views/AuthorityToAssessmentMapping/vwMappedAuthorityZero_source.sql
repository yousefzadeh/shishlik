with
    mapped as (
        select
            tam.TenantAuthorityMapping_SourceAuthorityId Direct_AuthorityId,
            tam.TenantAuthorityMapping_TargetAuthorityId Mapped_AuthorityId,
            auth.Authority_Name Mapped_AuthorityName
        from {{ ref("vwTenantAuthorityMapping") }} tam
        join
            {{ ref("vwAuthorityZero_source") }} auth on tam.TenantAuthorityMapping_TargetAuthorityId = auth.Authority_Id
    ),
    direct as (select Assessment_TenantId, Assessment_Id, Assessment_AuthorityId from {{ ref("vwAssessment") }}),
    final as (
        select
            direct.Assessment_Id,
            direct.Assessment_TenantId Tenant_Id,
            direct.Assessment_AuthorityId Direct_AuthorityId,
            coalesce(mapped.Mapped_AuthorityId, 0) Mapped_AuthorityId,
            coalesce(mapped.Mapped_AuthorityName, 'Unassigned Target Authority') Mapped_AuthorityName
        from direct
        left join mapped on direct.Assessment_AuthorityId = mapped.Direct_AuthorityId
    )
select *
from final

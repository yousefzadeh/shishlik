{#
  Coalesce causes query optimizer to be inconsistent and poor performing.
  Union inner join with outer join to overcome this
#}
with
    question_provision_authority_non_null as (
        select
            pq.ProvisionQuestion_QuestionId,
            map.AuthorityProvision_Id Mapped_AuthorityProvisionId,
            map.AuthorityProvision_AuthorityId Mapped_AuthorityId,
            a.Authority_Name Mapped_AuthorityName
        from {{ ref("vwProvisionQuestion") }} pq  -- direct provision
        inner join
            {{ ref("vwTenantAuthorityProvisionMapping") }} tapm
            -- direct provision                               -- source provision
            on pq.ProvisionQuestion_AuthorityProvisionId
            = tapm.TenantAuthorityProvisionMapping_SourceAuthorityProvisionId
            -- link to Tenant Id
            and pq.ProvisionQuestion_TenantId = tapm.TenantAuthorityProvisionMapping_TenantId
        inner join
            {{ ref("vwAuthorityProvision") }} map  -- mapped provision
            -- target mapping                                                    mapped provision   
            on tapm.TenantAuthorityProvisionMapping_TargetAuthorityProvisionId = map.AuthorityProvision_Id
        inner join {{ ref("vwAuthority") }} a on map.AuthorityProvision_AuthorityId = a.Authority_Id
    ),
    question_provision_authority_null as (
        select
            pq.ProvisionQuestion_QuestionId,
            0 Mapped_AuthorityProvisionId,
            0 Mapped_AuthorityId,
            'Unassigned Mapping (Authority Provision)' Mapped_AuthorityName
        from {{ ref("vwProvisionQuestion") }} pq  -- direct provision
        left join
            {{ ref("vwTenantAuthorityProvisionMapping") }} tapm
            -- direct provision                               -- source provision
            on pq.ProvisionQuestion_AuthorityProvisionId
            = tapm.TenantAuthorityProvisionMapping_SourceAuthorityProvisionId
            -- link to Tenant Id
            and pq.ProvisionQuestion_TenantId = tapm.TenantAuthorityProvisionMapping_TenantId
        left join
            {{ ref("vwAuthorityProvision") }} map  -- mapped provision
            -- target mapping                                                    mapped provision   
            on tapm.TenantAuthorityProvisionMapping_TargetAuthorityProvisionId = map.AuthorityProvision_Id
        left join {{ ref("vwAuthority") }} a on map.AuthorityProvision_AuthorityId = a.Authority_Id
        where map.AuthorityProvision_Id is null
    ),
    question_provision_authority as (
        select *
        from question_provision_authority_non_null
        union all
        select *
        from question_provision_authority_null
    ),
    question_control_provision_authority_non_null as (
        select
            cq.ControlQuestion_QuestionId,
            map.AuthorityProvision_Id Mapped_AuthorityProvisionId,
            map.AuthorityProvision_AuthorityId Mapped_AuthorityId,
            a.Authority_Name Mapped_AuthorityName
        from {{ ref("vwControlQuestion") }} cq  -- question control
        join
            {{ ref("vwProvisionControl") }} pc  -- control provision
            on cq.ControlQuestion_ControlsId = pc.ProvisionControl_ControlsId
        inner join
            {{ ref("vwTenantAuthorityProvisionMapping") }} tapm
            -- direct provision                               -- source provision
            on pc.ProvisionControl_AuthorityReferenceId
            = tapm.TenantAuthorityProvisionMapping_SourceAuthorityProvisionId
            -- link to Tenant Id
            and pc.ProvisionControl_TenantId = tapm.TenantAuthorityProvisionMapping_TenantId
        inner join
            {{ ref("vwAuthorityProvision") }} map  -- mapped provision
            -- target mapping                                                    mapped provision   
            on tapm.TenantAuthorityProvisionMapping_TargetAuthorityProvisionId = map.AuthorityProvision_Id
        inner join {{ ref("vwAuthority") }} a on map.AuthorityProvision_AuthorityId = a.Authority_Id
    ),
    question_control_provision_authority_null as (
        select
            cq.ControlQuestion_QuestionId,
            0 Mapped_AuthorityProvisionId,
            -1 Mapped_AuthorityId,
            'Unassigned Mapping (Authority Provision)' Mapped_AuthorityName
        from {{ ref("vwControlQuestion") }} cq  -- question control
        join
            {{ ref("vwProvisionControl") }} pc  -- control provision
            on cq.ControlQuestion_ControlsId = pc.ProvisionControl_ControlsId
        left join
            {{ ref("vwTenantAuthorityProvisionMapping") }} tapm
            -- direct provision                               -- source provision
            on pc.ProvisionControl_AuthorityReferenceId
            = tapm.TenantAuthorityProvisionMapping_SourceAuthorityProvisionId
            -- link to Tenant Id
            and pc.ProvisionControl_TenantId = tapm.TenantAuthorityProvisionMapping_TenantId
        left join
            {{ ref("vwAuthorityProvision") }} map  -- mapped provision
            -- target mapping                                                    mapped provision   
            on tapm.TenantAuthorityProvisionMapping_TargetAuthorityProvisionId = map.AuthorityProvision_Id
        left join {{ ref("vwAuthority") }} a on map.AuthorityProvision_AuthorityId = a.Authority_Id
        where map.AuthorityProvision_Id is null
    ),
    question_control_provision_authority as (
        select *
        from question_control_provision_authority_non_null
        union all
        select *
        from question_control_provision_authority_null
    ),
    final as (
        select
            'Provision' relation,
            ProvisionQuestion_QuestionId,
            Mapped_AuthorityProvisionId,
            Mapped_AuthorityId,
            Mapped_AuthorityName
        from question_provision_authority
        union all
        select
            'Control Provision' relation,
            ControlQuestion_QuestionId,
            Mapped_AuthorityProvisionId,
            Mapped_AuthorityId,
            Mapped_AuthorityName
        from question_control_provision_authority
    )
select *
from final

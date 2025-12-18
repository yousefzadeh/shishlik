with
    control_authority as (
        select distinct c.Controls_TenantId Tenant_Id, c.Controls_Id, a.Authority_Name
        from {{ ref("vwControls") }} c
        inner join
            {{ ref("vwProvisionControl") }} pc
            on c.Controls_Id = pc.ProvisionControl_ControlsId
        inner join
            {{ ref("vwDirectAuthorityProvision") }} p
            on pc.ProvisionControl_AuthorityReferenceId = p.AuthorityProvision_Id
            and pc.ProvisionControl_TenantId = p.Tenant_Id
        inner join
            {{ ref("vwDirectAuthority") }} a
            on p.Authority_Id = a.Authority_Id
            and p.Tenant_Id = a.Tenant_Id
    ),
    final as (
        select
            Tenant_Id,
            Controls_Id,
            string_agg(cast(Authority_Name as varchar(max)), ', ') authority_list
        from control_authority
        group by Tenant_Id, Controls_Id
    )
select *
from final

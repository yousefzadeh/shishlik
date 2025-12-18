with
    base as (
        select
            pc.ProvisionControl_TenantId Tenant_Id,
            Coalesce(pc.ProvisionControl_Id, 0) as ProvisionControl_Id,
            Coalesce(aprov.AuthorityProvision_Id, 0) as AuthorityProvision_Id,
            Coalesce(c.Controls_Id, 0) as Controls_Id,
            case
                when aprov.AuthorityProvision_Id is not NULL and c.Controls_Id is not NULL
                then 'Provision with Controls'
                when aprov.AuthorityProvision_Id is not NULL and c.Controls_Id is NULL
                then 'Provision with no Controls'
                when aprov.AuthorityProvision_Id is NULL and c.Controls_Id is not NULL
                then 'Control with no Provisions'
                when aprov.AuthorityProvision_Id is NULL and c.Controls_Id is NULL
                then 'ProvisionControl with no Provisions or Controls'
            end
            + case when pc.ProvisionControl_Id is NULL then ' and Unlinked' else '' end ProvisionToControl_Label,
            case
                when aprov.AuthorityProvision_Id is not NULL and c.Controls_Id is not NULL
                then 'Control with Provision'
                when aprov.AuthorityProvision_Id is not NULL and c.Controls_Id is NULL
                then 'Provision with no Controls'
                when aprov.AuthorityProvision_Id is NULL and c.Controls_Id is not NULL
                then 'Control with no Provisions'
                when aprov.AuthorityProvision_Id is NULL and c.Controls_Id is NULL
                then 'ProvisionControl with no Provisions or Controls'
            end
            + case when pc.ProvisionControl_Id is NULL then ' and Unlinked' else '' end ControlToProvision_Label
        from {{ ref("vwAuthorityProvision") }} aprov
        left join
            {{ ref("vwProvisionControl") }} pc  -- all provisions
            on aprov.AuthorityProvision_Id = pc.ProvisionControl_AuthorityReferenceId
        right join
            {{ ref("vwControls") }} c  -- all controls
            on pc.ProvisionControl_ControlsId = c.Controls_Id
    )

select *
from base

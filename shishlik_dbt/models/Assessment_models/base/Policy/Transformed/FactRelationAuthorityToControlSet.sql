with
    base as (
        select distinct
            AuthorityPolicy_TenantId Tenant_Id,
            coalesce(apol.AuthorityPolicy_Id, 0) as AuthorityPolicy_Id,
            coalesce(a.Authority_Id, 0) as Authority_Id,
            coalesce(cs.Controlset_Id, 0) as Controlset_Id,
            (
                case
                    when a.Authority_Id is not NULL and cs.Controlset_Id is not NULL
                    then 'Authority with Control Set'
                    when a.Authority_Id is not NULL and cs.Controlset_Id is NULL
                    then 'Authority with no Control Set'
                    when a.Authority_Id is NULL and cs.Controlset_Id is not NULL
                    then 'Control Set with no Authority'
                    when a.Authority_Id is NULL and cs.Controlset_Id is NULL
                    then 'AuthorityPolicy without Authority, Control Set'
                end
            ) + case
                when apol.AuthorityPolicy_Id is NULL then ' and Unlinked' else ''
            end AuthorityToControlSet_Label,
            (
                case
                    when a.Authority_Id is not NULL and cs.Controlset_Id is not NULL
                    then 'Control Set with Authority'
                    when a.Authority_Id is not NULL and cs.Controlset_Id is NULL
                    then 'Authority with no Control Set'
                    when a.Authority_Id is NULL and cs.Controlset_Id is not NULL
                    then 'Control Set with no Authority'
                    when a.Authority_Id is NULL and cs.Controlset_Id is NULL
                    then 'AuthorityPolicy without Authority, Control Set'
                end
            ) + case
                when apol.AuthorityPolicy_Id is NULL then ' and Unlinked' else ''
            end ControlSetToAuthority_Label
        from {{ ref("vwAuthority") }} a
        left join
            {{ ref("vwAuthorityPolicy") }} apol
            on a.Authority_Id = apol.AuthorityPolicy_AuthorityId
        right join
            {{ ref("vwControlset") }} cs
            on apol.AuthorityPolicy_PolicyId = cs.Controlset_Id
    )

select *
from base

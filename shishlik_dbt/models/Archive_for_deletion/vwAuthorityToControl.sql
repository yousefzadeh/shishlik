{{ config(materialized="view") }}

with
    auth_to_ctrl as (
        select
            a.Authority_Id,
            a.Authority_Name,
            ap.AuthorityProvision_Id,
            ap.AuthorityProvision_AuthorityId,
            ap.AuthorityProvision_ReferenceId,
            ap.AuthorityProvision_Name,
            c.Controls_Id,
            c.Controls_Reference,
            c.Controls_Name,
            pd.PolicyDomain_Id,
            pd.PolicyDomain_Name,
            c.Controls_Detail
        from {{ ref("vwAuthority") }} a
        join {{ ref("vwAuthorityProvision") }} ap on a.Authority_Id = ap.AuthorityProvision_AuthorityId
        join {{ ref("vwProvisionControl") }} pc on ap.AuthorityProvision_Id = pc.ProvisionControl_AuthorityReferenceId
        join {{ ref("vwControls") }} c on pc.ProvisionControl_ControlsId = c.Controls_Id
        join {{ ref("vwPolicyDomain") }} pd on pd.PolicyDomain_Id = c.Controls_PolicyDomainId
    )

select *
from auth_to_ctrl

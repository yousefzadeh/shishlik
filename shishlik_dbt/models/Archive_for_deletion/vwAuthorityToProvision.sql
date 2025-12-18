{{ config(materialized="view") }}

with
    auth_to_prov as (
        select
            a.Authority_Id,
            a.Authority_Name,
            ap.AuthorityProvision_Id,
            ap.AuthorityProvision_AuthorityId,
            ap.AuthorityProvision_ReferenceId,
            ap.AuthorityProvision_Name,
            pd.PolicyDomain_Id,
            pd.PolicyDomain_Name
        from {{ ref("vwAuthority") }} a
        join {{ ref("vwAuthorityProvision") }} ap on a.Authority_Id = ap.AuthorityProvision_AuthorityId
        join {{ ref("vwAuthorityPolicy") }} ap2 on ap2.AuthorityPolicy_AuthorityId = a.Authority_Id
        join {{ ref("vwPolicy") }} p on ap2.AuthorityPolicy_PolicyId = p.Policy_Id
        join {{ ref("vwPolicyDomain") }} pd on p.Policy_Id = pd.PolicyDomain_PolicyId
    )

select *
from auth_to_prov

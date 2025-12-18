with
    base_ctl as (
        select *
        from {{ ref("vwControls") }} c
        join {{ ref("vwPolicyDomain") }} pd on c.Controls_PolicyDomainId = pd.PolicyDomain_Id
    ),
    all_ctl as (
        select c.PolicyDomain_PolicyId, count(distinct c.Controls_Id) num_all_controls
        from base_ctl c
        group by c.PolicyDomain_PolicyId
    ),
    mapped_ctl as (
        select c.PolicyDomain_PolicyId, count(distinct c.Controls_Id) num_mapped_controls
        from base_ctl c
        join {{ ref("vwProvisionControl") }} pc on c.Controls_Id = pc.ProvisionControl_ControlsId
        group by c.PolicyDomain_PolicyId
    ),
    num_ctl as (
        select a.PolicyDomain_PolicyId Policy_Id, a.num_all_controls, m.num_mapped_controls
        from all_ctl a
        left join mapped_ctl m on a.PolicyDomain_PolicyId = m.PolicyDomain_PolicyId
    ),
    base_prov as (
        select *
        from {{ ref("vwAuthorityPolicy") }} ap
        join {{ ref("vwAuthority") }} a on ap.AuthorityPolicy_AuthorityId = a.Authority_Id
        join {{ ref("vwAuthorityProvision") }} ap1 on a.Authority_Id = ap1.AuthorityProvision_AuthorityId
    ),
    all_prov as (
        select p.AuthorityPolicy_PolicyId, count(distinct p.AuthorityProvision_Id) num_all_provision
        from base_prov p
        group by p.AuthorityPolicy_PolicyId
    ),
    mapped_prov as (
        select p.AuthorityPolicy_PolicyId, count(distinct p.AuthorityProvision_Id) num_mapped_provision
        from base_prov p
        join {{ ref("vwProvisionControl") }} pc on p.AuthorityProvision_Id = pc.ProvisionControl_AuthorityReferenceId
        join {{ ref("vwControls") }} c on pc.ProvisionControl_ControlsId = c.Controls_Id
        join {{ ref("vwPolicyDomain") }} pd on c.Controls_PolicyDomainId = pd.PolicyDomain_Id
        where p.AuthorityPolicy_PolicyId = pd.PolicyDomain_PolicyId
        group by p.AuthorityPolicy_PolicyId
    ),
    num_prov as (
        select a.AuthorityPolicy_PolicyId Policy_Id, a.num_all_provision, m.num_mapped_provision
        from all_prov a
        left join mapped_prov m on a.AuthorityPolicy_PolicyId = m.AuthorityPolicy_PolicyId
    ),
    auth_docs as (
        select distinct
            ap.AuthorityPolicy_PolicyId Policy_Id,
            count(*) num_auth_docs,
            string_agg(a.Authority_Name, ',') auth_docs_list
        from {{ ref("vwAuthorityPolicy") }} ap
        join {{ ref("vwAuthority") }} a on ap.AuthorityPolicy_AuthorityId = a.Authority_Id
        group by ap.AuthorityPolicy_PolicyId
    )

select
    c.Policy_Id ControlSet_Id,
    coalesce(c.num_all_controls, 0) num_all_controls,
    coalesce(c.num_mapped_controls, 0) num_mapped_controls,
    coalesce(p.num_all_provision, 0) num_all_provision,
    coalesce(p.num_mapped_provision, 0) num_mapped_provision,
    coalesce(a.num_auth_docs, 0) num_auth_docs,
    coalesce(a.auth_docs_list, '') auth_docs_list
from num_ctl c
left join num_prov p on c.Policy_Id = p.Policy_Id
left join auth_docs a on c.Policy_Id = a.Policy_Id

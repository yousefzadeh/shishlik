with
    map_doc as (
        select
            ap.AuthorityPolicy_TenantId Tenant_Id,
            ap.AuthorityPolicy_AuthorityId Authority_Id,
            p.Policy_Id ControlSet_Id,
            p.Policy_Name ControlSet_Name
        from {{ ref("vwAuthorityPolicy") }} ap
        join {{ ref("vwPolicy") }} p on ap.AuthorityPolicy_PolicyId = p.Policy_Id
    )

select *
from map_doc

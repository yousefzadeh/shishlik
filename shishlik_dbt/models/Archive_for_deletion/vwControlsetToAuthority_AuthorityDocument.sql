with
    map_doc as (
        select distinct ap.AuthorityPolicy_PolicyId ControlSet_Id, a.Authority_Id, a.Authority_Name
        from {{ ref("vwAuthorityPolicy") }} ap
        join {{ ref("vwAuthority") }} a on ap.AuthorityPolicy_AuthorityId = a.Authority_Id
    )

select *
from map_doc

with
    auth_prov as (
        select rp.RiskProvision_RiskId, a.Authority_Name, ap.AuthorityProvision_Name
        from {{ ref("vwRiskProvision") }} rp
        inner join
            {{ ref("vwAuthorityProvision") }} ap on rp.RiskProvision_AuthorityProvisionId = ap.AuthorityProvision_Id
        inner join {{ ref("vwAuthority") }} a on ap.AuthorityProvision_AuthorityId = a.Authority_Id
    ),
    auth_distinct as (select distinct RiskProvision_RiskId, Authority_Name from auth_prov)
select RiskProvision_RiskId, Authority_Name
from auth_distinct

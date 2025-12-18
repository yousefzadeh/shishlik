-- This view is an alias to the vwAuthorityProvisionCustomTable
-- This view should be deprecated
select * from {{ ref("vwAuthorityProvisionCustomTable") }}

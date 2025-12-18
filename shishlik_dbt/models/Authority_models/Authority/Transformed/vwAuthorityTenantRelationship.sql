-- To be deprecated
select Authority_Id, Tenant_Id, Method reason, Authority_Name from {{ ref("vwDirectAuthority") }}

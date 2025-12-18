{{ config(materialized="view") }}

{# 

 #}
select
    ta.TenantAuthority_TenantId,
    tam.TenantAuthorityMapping_TenantId,
    {# 
--ta.TenantAuthority_Id,
--tam.TenantAuthorityMapping_TargetAuthorityId Target_AuthId,
--tam.TenantAuthorityMapping_SourceAuthorityId Source_AuthId,
-- ta.TenantAuthority_AuthorityId,  
#}
    src.Authority_Id src_AuthorityId,
    src.Authority_TenantId src_TenantId,
    tgt.Authority_Id tgt_AuthorityId,
    tgt.Authority_TenantId tgt_TenantId
from {{ ref("vwTenantAuthority") }} ta
join
    {{ ref("vwTenantAuthorityMapping") }} tam
    on tam.TenantAuthorityMapping_SourceTenantAuthorityId = ta.TenantAuthority_Id
join {{ ref("vwAuthority") }} src on tam.TenantAuthorityMapping_SourceAuthorityId = src.Authority_Id
join {{ ref("vwAuthority") }} tgt on tam.TenantAuthorityMapping_TargetAuthorityId = tgt.Authority_Id

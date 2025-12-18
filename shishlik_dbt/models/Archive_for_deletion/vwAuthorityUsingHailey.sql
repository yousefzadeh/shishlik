-- Multi-valued relationship of Directly linked Authority to a similar Authority using Hailey 
select
    tam.TenantAuthorityMapping_TenantId,  -- Tenant 
    tam.TenantAuthorityMapping_SourceAuthorityId,  -- Linked Authority
    tam.TenantAuthorityMapping_TargetAuthorityId,  -- Similar Authority
    a.Authority_Name TenantAuthorityMapping_TargetAuthorityName,
    tam.TenantAuthorityMapping_SimilarityPercentage
from {{ ref("vwTenantAuthorityMapping") }} tam
join {{ ref("vwAuthority") }} a on tam.TenantAuthorityMapping_TargetAuthorityId = a.Authority_Id
where
    tam.TenantAuthorityMapping_SimilarityPercentage > 0
    -- Test case:
    -- and tam.TenantAuthorityMapping_TenantId = 3 -- Login tenant
    -- and tam.TenantAuthorityMapping_SourceAuthorityId = 152 -- linked authority
    

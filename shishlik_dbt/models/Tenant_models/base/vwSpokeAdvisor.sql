-- Advisors by Spoke
select distinct
TenantVendor_TenantId Hub_Id,
TenantVendor_VendorId Spoke_ID,
AbpUsers_Id Advisor_Id,
AbpUsers_FullName AdvisorName
from {{ ref("vwTenantVendor") }} tv 
join {{ ref("vwAbpUser") }} au on tv.TenantVendor_VendorId = au.AbpUsers_TenantId
join {{ ref("vwAbpUserRoles") }} aur on au.AbpUsers_Id = aur.AbpUserRoles_UserId
join {{ ref("vwAbpRoles") }} ar on aur.AbpUserRoles_RoleId = ar.AbpRoles_Id
where ar.AbpRoles_Name = 'Advisor'

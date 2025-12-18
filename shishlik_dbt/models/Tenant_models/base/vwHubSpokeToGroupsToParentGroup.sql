SELECT
   Hub_Spoke_to_Groups.Rpt_TenantVendorGroup_VendorGroupId,
   Hub_Spoke_to_Groups.Rpt_TenantVendorGroup_SpokeId,
   Group_to_Parent.*,
   CASE Hub_Spoke_to_Groups.Rpt_TenantVendorGroup_VendorGroupId
      WHEN -1 THEN 'ALL'
      WHEN 0 THEN 'Unassigned'
      WHEN NULL THEN 'Not In Spoke Group'
      ELSE Group_to_Parent.VendorGroup_Name
   END VendorGroup_NameCalc
FROM {{ ref("vwRpt_TenantVendorGroup") }} AS Hub_Spoke_to_Groups
LEFT OUTER JOIN {{ ref("vwVendorGroup") }} AS Group_to_Parent
ON Hub_Spoke_to_Groups.Rpt_TenantVendorGroup_VendorGroupId = Group_to_Parent.VendorGroup_Id
and Hub_Spoke_to_Groups.Rpt_TenantVendorGroup_TenantId = Group_to_Parent.VendorGroup_TenantId




with auth_prov as (
    select 
    Tenant_Id,
    Authority_Id,
    AuthorityProvision_Id
    from {{ ref("vwDirectAuthorityProvision") }}
),
controlset_control as (
	select  
	Policy_TenantId,
	Policy_Id,
	Policy_Name,
	PolicyDomain_Id,
	Controls_Id,
	Controls_Reference,
	Controls_Name 
	from {{ ref("vwControlsetControl") }} 
),
{#- All provisions with mapping to another provision #}
target_provision as (
    select distinct 
    mapping.TenantAuthorityProvisionMapping_TenantId Tenant_Id,
    src_ap.Authority_Id,           
    src_ap.AuthorityProvision_Id,  
    tgt_ap.Authority_Id TargetAuthority_Id,
    tgt_ap.Authority_Name TargetAuthority_Name,
    tgt_ap.AuthorityProvision_Id TargetAuthorityProvision_Id,
    tgt_ap.AuthorityProvision_ReferenceId TargetAuthorityProvision_ReferenceId,
    tgt_ap.AuthorityProvision_Name TargetAuthorityProvision_Name
    from auth_prov src_ap 
    join {{ ref("vwTenantAuthorityProvisionMapping") }} mapping -- source, target 
      on src_ap.Authorityprovision_Id = mapping.TenantAuthorityProvisionMapping_SourceAuthorityProvisionId
      and src_ap.Tenant_Id = mapping.TenantAuthorityProvisionMapping_TenantId
    join {{ ref("vwDirectAuthorityProvision") }} tgt_ap 
      on tgt_ap.AuthorityProvision_Id = mapping.TenantAuthorityProvisionMapping_TargetAuthorityProvisionId
      and tgt_ap.Tenant_Id = mapping.TenantAuthorityProvisionMapping_TenantId
),
final as (
	select distinct
  {#- source #}
  tp.Tenant_Id, 
  tp.Authority_Id,
  tp.AuthorityProvision_Id,
  {#- target #}
  csc.Policy_Id TargetControlSet_Id,
  csc.Policy_Name TargetControlSet_Name,
  csc.Controls_Id TargetControl_Id,
	csc.Controls_Reference TargetControl_Reference,
	csc.Controls_Name TargetControl_Name
	from target_provision tp
  {#- Joining on TenantId in the next 3 joins creates a performance issue #}
	join {{ ref("vwProvisionControl") }} pc on tp.TargetAuthorityProvision_Id = pc.ProvisionControl_AuthorityReferenceId and tp.Tenant_Id = pc.ProvisionControl_TenantId
	join {{ ref("vwAuthorityPolicy") }} apol on tp.TargetAuthority_Id = apol.AuthorityPolicy_AuthorityId and tp.Tenant_Id = apol.AuthorityPolicy_TenantId
	join controlset_control csc on pc.ProvisionControl_ControlsId = csc.Controls_Id and apol.AuthorityPolicy_PolicyId = csc.Policy_Id 
)
select 
Tenant_Id, 
Authority_Id,
AuthorityProvision_Id,
TargetControlSet_Id,
TargetControlSet_Name,
TargetControl_Id,
TargetControl_Reference,
TargetControl_Name 
from final


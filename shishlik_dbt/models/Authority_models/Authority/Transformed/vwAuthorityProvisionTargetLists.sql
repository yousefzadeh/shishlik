-- Target Control List ([Ctrl Set 1: Ctrl 1, Ctrl 2] - as above)
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
target_provision as (
    select distinct 
    Tenant_Id,
    Authority_Id,            -- Source
    AuthorityProvision_Id,  -- Source
    TargetAuthority_Id,
    TargetAuthority_Name,
    TargetAuthorityProvision_Id,
    TargetAuthorityProvision_ReferenceId,
    TargetAuthorityProvision_Name
    from {{ ref("vwAuthorityProvisionToTargetAuthorityProvision") }}
),
target_control as (
	select distinct
    Tenant_Id,
    Authority_Id,
    AuthorityProvision_Id,
    TargetControlSet_Name,
	TargetControl_Reference,
	TargetControl_Name
	from {{ ref("vwAuthorityProvisionToTargetControlSetControl")}}
),
provision_risk as (
    select distinct 
    ap.Tenant_Id,
    ap.Authority_Id,
    ap.AuthorityProvision_Id,
    r.Risk_Name
    from auth_prov ap 
    join {{ ref("vwRiskProvision") }} rp on RiskProvision_AuthorityProvisionId = ap.AuthorityProvision_Id
    join {{ ref("vwRisk") }} r on rp.RiskProvision_RiskId = r.Risk_Id
),
provision_issue as (
    select distinct 
    ap.Tenant_Id,
    ap.Authority_Id,
    ap.AuthorityProvision_Id,
    i.Issues_Name
    from auth_prov ap 
    join {{ ref("vwIssueProvision") }} ip on IssueProvision_AuthorityProvisionId = ap.AuthorityProvision_Id
    join {{ ref("vwIssues") }} i on ip.IssueProvision_IssueId = i.Issues_Id
),
{# 
    Construct the lists 
    - target authorities and provisions
    - target controlset and controls 
    - related issues
    - related risks
 #}
target_authority_provision_list as ( -- Stage 1
    select 
    Tenant_Id, 
    Authority_Id,
    AuthorityProvision_Id, -- source
    TargetAuthority_Name,  -- Target
    string_agg(cast(TargetAuthorityProvision_ReferenceId as varchar(max)),', ') TargetAuthorityProvision_RefList, 
    string_agg(cast(TargetAuthorityProvision_Name as varchar(max)),', ') TargetAuthorityProvision_List 
    from target_provision 
    group by Tenant_Id, Authority_Id, AuthorityProvision_Id, TargetAuthority_Name
),
target_provision_list as ( -- Stage 2
    select 
    Tenant_Id, 
    Authority_Id,
    AuthorityProvision_Id, -- source
    string_agg(CAST('['+ (TargetAuthority_Name + ': ' + TargetAuthorityProvision_RefList)+']' as varchar(max)),', ') TargetAuthorityProvision_RefList, 
    string_agg(CAST('['+ (TargetAuthority_Name + ': ' + TargetAuthorityProvision_List)+']' as varchar(max)),', ') TargetAuthorityProvision_List 
    from target_authority_provision_list 
    group by Tenant_Id, Authority_Id, AuthorityProvision_Id
),
target_controlset_control_list as ( 
	select 
    Tenant_Id,
    Authority_Id,
    AuthorityProvision_Id,
    TargetControlSet_Name,
    string_agg(cast(TargetControl_Reference as varchar(max)), ', ') TargetControl_RefList,
    string_agg(cast(TargetControl_Name as varchar(max)), ', ') TargetControl_NameList
	from target_control 
	group by 
    Tenant_Id,
    Authority_Id,
    AuthorityProvision_Id,
    TargetControlSet_Name    
),
target_control_list as ( 
	select 
    Tenant_Id,
    Authority_Id,
    AuthorityProvision_Id,
    string_agg(cast(('['+TargetControlSet_Name+': '+TargetControl_RefList+']') as varchar(max)), ', ') TargetControl_RefList,
    string_agg(cast(('['+TargetControlSet_Name+': '+TargetControl_NameList+']') as varchar(max)), ', ') TargetControl_List
	from target_controlset_control_list 
	group by 
    Tenant_Id,
    Authority_Id,
    AuthorityProvision_Id    
),
issue_list as (
    select 
    Tenant_Id,
    Authority_Id,
    AuthorityProvision_Id,
    string_agg(cast(Issues_Name as varchar(max)), ', ') Issue_List
    from provision_issue pi 
    group by
    Tenant_Id,
    Authority_Id,
    AuthorityProvision_Id
),
risk_list as (
    select 
    Tenant_Id,
    Authority_Id,
    AuthorityProvision_Id,
    string_agg(cast(Risk_Name as varchar(max)), ', ') Risk_List
    from provision_risk pr 
    group by 
    Tenant_Id,
    Authority_Id,
    AuthorityProvision_Id
),
final as ( 
    select 
    ap.Tenant_Id,
    ap.Authority_Id,
    ap.AuthorityProvision_Id,
    TargetAuthorityProvision_RefList,
    TargetAuthorityProvision_List,
    TargetControl_RefList,
    TargetControl_List,
    Issue_List,
    Risk_List
    from auth_prov ap 
    left join target_provision_list tp on tp.Tenant_Id = ap.Tenant_Id and tp.Authority_Id = ap.Authority_Id and tp.AuthorityProvision_Id = ap.AuthorityProvision_Id  
    left join target_control_list tc on tc.Tenant_Id = ap.Tenant_Id and tc.Authority_Id = ap.Authority_Id and tc.AuthorityProvision_Id = ap.AuthorityProvision_Id
    left join issue_list i on i.Tenant_Id = ap.Tenant_Id and i.Authority_Id = ap.Authority_Id and i.AuthorityProvision_Id = ap.AuthorityProvision_Id
    left join risk_list r on r.Tenant_Id = ap.Tenant_Id and r.Authority_Id = ap.Authority_Id and r.AuthorityProvision_Id = ap.AuthorityProvision_Id
)
select *
from final -- too long
{# where Tenant_Id = 1384 -- 2874 rows #}


with target_auth as (
    select distinct
    da.Tenant_Id,
    da.Authority_Id,
    da.Authority_Name Authority_Name,
    target_auth.Authority_Name  TargetAuthority_Name
    from {{ ref("vwDirectAuthority") }} da 
    join {{ ref("vwTenantAuthorityMapping") }} tam 
    on da.Tenant_Id = tam.TenantAuthorityMapping_TenantId 
    and da.Authority_Id = tam.TenantAuthorityMapping_SourceAuthorityId 
    join {{ ref("vwAuthority") }} target_auth 
    on tam.TenantAuthorityMapping_TargetAuthorityId = target_auth.Authority_Id
),
target_cs as (
    select distinct
    da.Tenant_Id,
    da.Authority_Id,
    da.Authority_Name,
    cs.Policy_Name 
    from {{ ref("vwDirectAuthority") }} da 
    join {{ ref("vwTenantAuthorityMapping") }} tam 
    on da.Tenant_Id = tam.TenantAuthorityMapping_TenantId 
    and da.Authority_Id = tam.TenantAuthorityMapping_SourceAuthorityId 
    join {{ ref("vwAuthorityPolicy") }} target_cs 
    on tam.TenantAuthorityMapping_TargetAuthorityId = target_cs.AuthorityPolicy_AuthorityId 
    join {{ ref("vwPolicy") }} cs 
    on target_cs.AuthorityPolicy_PolicyId = cs.Policy_Id 
),
auth_ass as (
    select distinct 
    da.Tenant_Id,
    da.Authority_Id,
    da.Authority_Name,
    ass.Assessment_Name
    from {{ ref("vwDirectAuthority") }} da
    join {{ ref("vwAssessment") }} ass 
      on da.Authority_Id = ass.Assessment_AuthorityId
    where ass.Assessment_IsTemplate = 0
), 
auth_ass_template as (
    select distinct 
    da.Tenant_Id,
    da.Authority_Id,
    da.Authority_Name,
    ass.Assessment_Name
    from {{ ref("vwDirectAuthority") }} da
    join {{ ref("vwAssessment") }} ass 
      on da.Authority_Id = ass.Assessment_AuthorityId
    where ass.Assessment_IsTemplate = 1
), 
target_auth_list as (
    select 
    Tenant_Id, 
    Authority_Id,
    Authority_Name,
    string_agg(TargetAuthority_Name,', ') TargetAuthority_List 
    from target_auth 
    group by Tenant_Id, Authority_Id, Authority_Name 
),
target_cs_list as (
    select 
    Tenant_Id, 
    Authority_Id,
    Authority_Name,
    string_agg(cast(Policy_Name as varchar(max)),', ') TargetControlset_List 
    from target_cs 
    group by Tenant_Id, Authority_Id, Authority_Name 
),
auth_ass_list as (
    select 
    Tenant_Id, 
    Authority_Id,
    Authority_Name,
    string_agg(cast(Assessment_Name as varchar(max)),', ') Assessment_List 
    from auth_ass 
    group by Tenant_Id, Authority_Id, Authority_Name 
),
auth_ass_template_list as (
    select 
    Tenant_Id, 
    Authority_Id,
    Authority_Name,
    string_agg(cast(Assessment_Name as varchar(max)),', ') Template_List 
    from auth_ass_template 
    group by Tenant_Id, Authority_Id, Authority_Name 
)
, final as (
select 
coalesce(ass.Tenant_Id, temp.Tenant_Id, auth.Tenant_Id, cs.Tenant_Id) Tenant_Id,
coalesce(ass.Authority_Id, temp.Authority_Id, auth.Authority_Id, cs.Authority_Id) Authority_Id,
ass.Assessment_List, 
temp.Template_List, 
TargetAuthority_List,
TargetControlset_List
from auth_ass_list ass   
full outer join target_auth_list auth on ass.Tenant_Id = auth.Tenant_Id and ass.Authority_Id = auth.Authority_Id 
full outer join target_cs_list cs on ass.Tenant_Id = cs.Tenant_Id and ass.Authority_Id = cs.Authority_Id
full outer join auth_ass_template_list temp on ass.Tenant_Id = temp.Tenant_Id and ass.Authority_Id = temp.Authority_Id
)
select * 
from final
{# where Tenant_Id = 1384 #}

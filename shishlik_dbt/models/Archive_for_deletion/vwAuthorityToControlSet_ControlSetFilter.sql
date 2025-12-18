-- Authority to Control Set Compliance Report
-- Summary Box
-- Context of this report is Filtered by 
-- Tenant_Id - access filtering
-- Authority_Id -- User prompt by Authority_Name - choose one 
-- Policy_Id -- User prompt by Policy_Name - choose none or many
-- Authority can be direct or related
with
    auth as (
        select distinct T.*
        from
            (
                select
                    a.Authority_Id,
                    a.Authority_Name,
                    ta.TenantAuthority_TenantId Tenant_Id,
                    'Related' Authority_Relation
                from {{ ref("vwTenantAuthority") }} ta
                join {{ ref("vwAuthority") }} a on a.Authority_Id = ta.TenantAuthority_AuthorityId

                union all

                -- Tenant Id from Tenant Table
                select Authority_Id, Authority_Name, Authority_TenantId, 'Direct' Authority_Relation
                from {{ ref("vwAuthority") }}
            ) as T
    ),
    -- Provisions
    -- Get total number of provisions in this authority ('Number of Provisions')
    all_provisions as (
        select distinct
            auth.Tenant_Id,
            ap.AuthorityProvision_AuthorityId Authority_Id,
            ap.AuthorityProvision_Id,
            ap.AuthorityProvision_ReferenceId + '_' + ap.AuthorityProvision_Name AuthorityProvision_RefName
        from {{ ref("vwAuthorityProvision") }} ap
        join auth on ap.AuthorityProvision_AuthorityId = auth.Authority_Id
    {#- where ap.AuthorityProvision_AuthorityId = 10 and t.Tenant_Id = 3 #}
    ),
    num_all_provisions as (
        select
            Tenant_Id,
            Authority_Id,
            'Number of Provisions' label,
            count(DISTINCT AuthorityProvision_Id) count_id,
            count(DISTINCT AuthorityProvision_RefName) count_name
        from all_provisions
        group by Tenant_Id, Authority_Id
    ),
    -- Get total number of mapped provisions in this authority ('Number of Mapped Provisions')
    mapped_provisions as (
        select distinct
            pc.ProvisionControl_TenantId Tenant_Id,
            ap.AuthorityProvision_AuthorityId Authority_Id,
            pd.PolicyDomain_PolicyId Policy_Id,
            ap.AuthorityProvision_Id,
            ap.AuthorityProvision_ReferenceId + '_' + ap.AuthorityProvision_Name AuthorityProvision_RefName
        from {{ ref("vwAuthorityProvision") }} ap
        join {{ ref("vwProvisionControl") }} pc on ap.AuthorityProvision_Id = pc.ProvisionControl_AuthorityReferenceId
        join {{ ref("vwControls") }} c on pc.ProvisionControl_ControlsId = c.Controls_Id
        join {{ ref("vwPolicyDomain") }} pd on c.Controls_PolicyDomainId = pd.PolicyDomain_Id
        join {{ ref("vwPolicy") }} p on pd.PolicyDomain_PolicyId = p.Policy_Id
        where p.Policy_IsCurrent = 1
     {#- and ap.AuthorityProvision_AuthorityId = 10 and pc.ProvisionControl_TenantId = 3
     and pd.PolicyDomain_PolicyId in (1364,2027,1727) #}
    ),
    num_mapped_provisions as (
        select
            Tenant_Id,
            Authority_Id,
            'Number of Mapped Provisions' label,
            count(DISTINCT AuthorityProvision_Id) count_id,
            count(DISTINCT AuthorityProvision_RefName) count_name
        from mapped_provisions
        group by Tenant_Id, Authority_Id
    ),
    -- Control Sets
    -- Get control sets mapped to this authority ('Mapped Control Sets')
    mapped_controlset as (
        select distinct
            ap.AuthorityPolicy_TenantId Tenant_Id,
            ap.AuthorityPolicy_AuthorityId Authority_Id,
            p.Policy_Id,
            p.Policy_Name
        from {{ ref("vwAuthorityPolicy") }} ap
        join {{ ref("vwPolicy") }} p on ap.AuthorityPolicy_PolicyId = p.Policy_Id
        where p.Policy_IsCurrent = 1
    {#- and ap.AuthorityPolicy_AuthorityId = 10 and ap.AuthorityPolicy_TenantId = 3 
    and p.Policy_Id in (1364,2027,1727) #}
    ),
    list_mapped_controlset as (
        select
            Tenant_Id,
            Authority_Id,
            'Control Sets linked to Authority' label,
            count(DISTINCT Policy_Id) count_id,
            count(DISTINCT Policy_Name) count_name,
            string_agg(Policy_Name, ',') list_control_set
        from mapped_controlset
        group by Tenant_Id, Authority_Id
    ),
    -- Controls
    -- Get total number of controls in the control sets mapped to this control set ('Total Number of Controls')
    all_controls as (
        select distinct
            ap.AuthorityPolicy_TenantId Tenant_Id,
            ap.AuthorityPolicy_AuthorityId Authority_Id,
            ap.AuthorityPolicy_PolicyId Policy_Id,
            c.Controls_Id,
            c.Controls_Reference + '_' + c.Controls_Name Controls_RefName
        from {{ ref("vwAuthorityPolicy") }} ap
        join {{ ref("vwPolicy") }} p on ap.AuthorityPolicy_PolicyId = p.Policy_Id
        join {{ ref("vwPolicyDomain") }} pd on p.Policy_Id = pd.PolicyDomain_PolicyId
        join {{ ref("vwControls") }} c on pd.PolicyDomain_Id = c.Controls_PolicyDomainId
        where p.Policy_IsCurrent = 1
    {#- and ap.AuthorityPolicy_AuthorityId = 10 and ap.AuthorityPolicy_TenantId = 3 
    and ap.AuthorityPolicy_PolicyId in (1364,2027,1727) #}
    ),
    num_all_controls as (
        select
            Tenant_Id,
            Authority_Id,
            'Total Number of Controls in Control Sets Linked to Authority' label,
            count(DISTINCT Controls_Id) count_id,
            count(DISTINCT Controls_RefName) count_name
        from all_controls
        group by Tenant_Id, Authority_Id
    ),
    -- Get total number of mapped controls in the control sets mapped to this authority ('Number of Mapped Controls')
    mapped_controls as (
        select distinct
            ap.AuthorityPolicy_TenantId Tenant_Id,
            ap1.AuthorityProvision_AuthorityId Authority_Id,
            pd.PolicyDomain_PolicyId Policy_Id,
            c.Controls_Id,
            c.Controls_Reference + '_' + c.Controls_Name Controls_RefName
        from {{ ref("vwAuthorityPolicy") }} ap
        join {{ ref("vwPolicy") }} p on ap.AuthorityPolicy_PolicyId = p.Policy_Id
        join {{ ref("vwPolicyDomain") }} pd on p.Policy_Id = pd.PolicyDomain_PolicyId
        join {{ ref("vwControls") }} c on pd.PolicyDomain_Id = c.Controls_PolicyDomainId
        join {{ ref("vwProvisionControl") }} pc on c.Controls_Id = pc.ProvisionControl_ControlsId
        join
            {{ ref("vwAuthorityProvision") }} ap1
            on pc.ProvisionControl_AuthorityReferenceId = ap1.AuthorityProvision_Id
        where ap.AuthorityPolicy_AuthorityId = ap1.AuthorityProvision_AuthorityId and p.Policy_IsCurrent = 1
    {# and ap1.AuthorityProvision_AuthorityId = 10 and ap.AuthorityPolicy_TenantId = 3
    and pd.PolicyDomain_PolicyId in (1364,2027,1727) #}
    ),
    num_mapped_controls as (
        select
            Tenant_Id,
            Authority_Id,
            'Number of Mapped Controls' label,
            count(DISTINCT Controls_Id) count_id,
            count(DISTINCT Controls_RefName) count_name
        from mapped_controls
        group by Tenant_Id, Authority_Id
    ),
    -- Test query to match Joels query with Tenant and Authority filter only
    -- , row_wise as (
    -- select 
    -- all_p.Tenant_Id,
    -- all_p.Authority_Id,
    -- all_p.count_id num_all_provisions,
    -- map_p.count_id num_mapped_provisions,
    -- map_cs.count_id num_mapped_control_set,
    -- map_cs.list_control_set,
    -- all_c.count_id num_all_controls,
    -- map_c.count_id num_mapped_controls
    -- from num_all_provisions all_p
    -- join num_mapped_provisions map_p
    -- on all_p.Tenant_Id = map_p.Tenant_Id and all_p.Authority_Id = map_p.Authority_Id 
    -- join list_mapped_controlset map_cs 
    -- on all_p.Tenant_Id = map_cs.Tenant_Id and all_p.Authority_Id = map_cs.Authority_Id
    -- full outer join num_all_controls all_c
    -- on all_p.Tenant_Id = all_c.Tenant_Id and all_p.Authority_Id = all_c.Authority_Id
    -- join num_mapped_controls map_c 
    -- on all_c.Tenant_Id = map_c.Tenant_Id and all_c.Authority_Id = map_c.Authority_Id
    -- )
    -- Matched with Joel's result without controlset filter
    -- , no_controlset_filter as (
    -- select auth.Authority_Name, row_wise.*
    -- from row_wise
    -- right join auth on row_wise.Authority_Id = auth.Authority_Id and row_wise.Tenant_Id =  auth.Tenant_Id
    -- where Tenant_Id = 3 and Authority_Id = 10
    -- )
    flat_controls_detail as (
        select
            a.Tenant_Id,
            a.Authority_Id,
            a.Policy_Id,
            'Controls' ClassName,
            a.Controls_Id ClassId,
            m.Controls_Id MappedClassId,
            m.Controls_RefName MappedClassName
        from all_controls a
        left join
            mapped_controls m
            on a.Controls_Id = m.Controls_Id
            and a.Authority_Id = m.Authority_Id
            and a.Tenant_Id = m.Tenant_Id
    ),
    flat_provisions_detail as (
        select
            a.Tenant_Id,
            a.Authority_Id,
            m.Policy_Id,
            'Provisions' ClassName,
            a.AuthorityProvision_Id ClassId,
            m.AuthorityProvision_Id MappedClassId,
            m.AuthorityProvision_RefName MappedClassName
        from all_provisions a
        left join
            mapped_provisions m
            on a.AuthorityProvision_Id = m.AuthorityProvision_Id
            and a.Authority_Id = m.Authority_Id
            and a.Tenant_Id = m.Tenant_Id
    ),
    flat_mapped_controlset_detail as (
        select
            m.Tenant_Id,
            m.Authority_Id,
            m.Policy_Id,
            'Control Sets' ClassName,
            NULL ClassId,
            m.Policy_Id MappedClassId,
            m.Policy_Name MappedClassName
        from mapped_controlset m
    ),
    uni_detail as (
        select *
        from flat_controls_detail

        union all

        select *
        from flat_provisions_detail

        union all

        select *
        from flat_mapped_controlset_detail
    )
-- Test query
-- , uni_agg as (
-- select 
-- Tenant_Id,
-- Authority_Id,
-- ClassName,
-- count(distinct id) all_count,
-- count(distinct mapped_id) mapped_count,
-- string_agg(mapped_name,',') mapped_list
-- from uni_detail
-- where Tenant_Id = 3 and Authority_Id = 10
-- and Policy_Id in (1364,2027,1727)
-- group BY 
-- Tenant_Id,
-- Authority_Id,
-- ClassName
-- all  map
-- "Control Sets"	0	3
-- "Controls"		28	2
-- "Provisions" 		3	3
-- )
select *
from
    uni_detail
{#- where Tenant_Id = 3 and Authority_Id = 10
and Policy_Id in (1364,2027,1727) #}
    

{{ config(materialized="view") }}

-- AuthorityToControlsetMapping
with
    auth as (
        select distinct t.*
        from
            (
                select
                    'Tenant Authority' source_table,
                    TenantAuthority_AuthorityId Authority_Id,
                    a.Authority_Name,
                    ta.TenantAuthority_TenantId Authority_TenantId,
                    a.Authority_Status,
                    a.Authority_IsArchived
                from {{ ref("vwTenantAuthority") }} ta
                join {{ ref("vwAuthority") }} a on a.Authority_Id = ta.TenantAuthority_AuthorityId

                union all

                select
                    'Authority' source_table,
                    Authority_Id,
                    Authority_Name,
                    Authority_TenantId,
                    a.Authority_Status,
                    a.Authority_IsArchived
                from {{ ref("vwAuthority") }} a
            ) as t
    ),
    ctrl as (
        select *
        from {{ ref("vwControls") }} c
        join {{ ref("vwPolicyDomain") }} pd on pd.PolicyDomain_Id = c.Controls_PolicyDomainId
        join {{ ref("vwPolicy") }} p on pd.PolicyDomain_PolicyId = p.Policy_Id
    ),
    auth_prov as (
        select *
        from auth a
        -- provision
        join {{ ref("vwAuthorityProvision") }} ap on a.Authority_Id = ap.AuthorityProvision_AuthorityId
    ),
    auth_to_ctrl as (
        select
            'Provisions linked to controls' Provisions_to_Controls_Link_Type,
            ap.source_table,
            ap.Authority_TenantId,
            ap.Authority_Id,
            ap.Authority_Name,
            ap.Authority_Status,
            ap.Authority_IsArchived,
            ap.AuthorityProvision_Id,
            ap.AuthorityProvision_AuthorityId,
            ap.AuthorityProvision_ReferenceId,
            ap.AuthorityProvision_Name,
            c.Controls_Id,
            c.Controls_Reference,
            c.Controls_Name,
            c.PolicyDomain_Id,
            c.PolicyDomain_Name,
            c.Controls_Detail,
            c.Controls_IsCurrent,
            c.Policy_Id,
            c.Policy_Name
        from auth_prov ap
        join
            {{ ref("vwProvisionControl") }} pc
            on ap.AuthorityProvision_Id = pc.ProvisionControl_AuthorityReferenceId
            and ap.Authority_TenantId = pc.ProvisionControl_TenantId
        -- controlset
        join ctrl c on pc.ProvisionControl_ControlsId = c.Controls_Id
        where ap.Authority_Status = 2 and ap.Authority_IsArchived = 0
    ),
    auth_not_ctrl as (
        select
            'Provisions not linked to controls' Provisions_to_Controls_Link_Type,
            ap.source_table,
            ap.Authority_TenantId,
            ap.Authority_Id,
            ap.Authority_Name,
            ap.Authority_Status,
            ap.Authority_IsArchived,
            ap.AuthorityProvision_Id,
            ap.AuthorityProvision_AuthorityId,
            ap.AuthorityProvision_ReferenceId,
            ap.AuthorityProvision_Name,
            NULL Controls_Id,
            NULL Controls_Reference,
            NULL Controls_Name,
            NULL PolicyDomain_Id,
            NULL PolicyDomain_Name,
            NULL Controls_Detail,
            NULL Controls_IsCurrent,
            NULL Policy_Id,
            NULL Policy_Name
        from auth_prov ap
        left join
            {{ ref("vwProvisionControl") }} pc
            on ap.AuthorityProvision_Id = pc.ProvisionControl_AuthorityReferenceId
            and ap.Authority_TenantId = pc.ProvisionControl_TenantId
        -- controlset
        left join ctrl c on pc.ProvisionControl_ControlsId = c.Controls_Id
        where ap.Authority_Status = 2 and ap.Authority_IsArchived = 0 and (c.Controls_Id is NULL or c.Policy_Id is NULL)
    ),
    auth_ctrl as (
        select *
        from auth_to_ctrl
        union all
        select *
        from auth_not_ctrl
    ),
    [description] as (
        select *
        from {{ ref("vwAuthorityProvisionCustomTable") }} apct
        where AuthorityProvisionCustomField_FieldName = 'Description'
    ),
    custom as (
        select *
        from {{ ref("vwAuthorityProvisionCustomTable") }} apct
        where AuthorityProvisionCustomField_FieldName not in ('Description')
    )
select distinct
    a.Provisions_to_Controls_Link_Type,
    a. [source_table],
    a.Authority_TenantId,
    a.Authority_Id,
    a.Authority_Name,
    a.Authority_Status,
    a.AuthorityProvision_Id,
    a.AuthorityProvision_ReferenceId,
    a.AuthorityProvision_Name,
    d.AuthorityProvisionCustomField_FieldNameValue ProvisionDescription,
    c.AuthorityProvisionCustomField_FieldName FieldName,
    c.AuthorityProvisionCustomField_FieldNameValue Value,
    coalesce(a.Policy_Name, 'Not Linked') ControlSet_Name,
    coalesce(a.Policy_Id, 0) ControlSet_Id,
    coalesce(a.Controls_Id, 0) Controls_Id,
    coalesce(a.Controls_Reference, 'Not Linked') Control_RefID,
    coalesce(a.Controls_Name, 'Not Linked') ControlName,
    coalesce(a.PolicyDomain_Id, 0) PolicyDomain_Id,
    coalesce(a.PolicyDomain_Name, 'Not Linked') ControlDomain,
    coalesce(a.Controls_Detail, 'Control is not linked') ControlDescription,
    coalesce(a.Controls_IsCurrent, 1) control_iscurrent_flag
from auth_ctrl a
left join [description] d on d.AuthorityProvision_Id = a.AuthorityProvision_Id
left join
    custom c on c.AuthorityProvision_Id = a.AuthorityProvision_Id
    -- where a.Policy_Id is not NULL
    

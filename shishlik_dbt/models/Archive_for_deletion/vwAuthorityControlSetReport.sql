{{ config(materialized="view") }}

with
    ap as (
        select *
        from {{ ref("vwAuthority") }} a
        join {{ ref("vwAuthorityProvision") }} ap on ap.AuthorityProvision_AuthorityId = a.Authority_Id
    -- where a.Authority_Id = 63 -- 312 rows
    ),
    custom as (
        select distinct
            ap.AuthorityProvision_AuthorityId,
            ap.AuthorityProvision_Id,
            apct.AuthorityProvisionCustomField_FieldName CustomFieldName,
            apct.AuthorityProvisionCustomField_FieldNameValue CustomValue,
            apct.AuthorityProvisionCustomField_Order
        from {{ ref("vwAuthorityProvision") }} ap
        join {{ ref("vwAuthorityProvisionCustomTable") }} apct on apct.AuthorityProvision_Id = ap.AuthorityProvision_Id
    -- where ap.AuthorityProvision_AuthorityId = 63 -- 1560 rows
    -- and apct.AuthorityProvisionCustomField_Order = 5 -- 312 rows
    ),
    pc as (
        select pc.*, tat.tgt_TenantId, tat.tgt_AuthorityId
        from {{ ref("vwProvisionControl") }} pc
        join {{ ref("vwTenantAuthorityTenant") }} tat on pc.ProvisionControl_TenantId = tat.TenantAuthority_TenantId
    ),
    c as (
        select c.*, tat.tgt_TenantId, tat.tgt_AuthorityId
        from {{ ref("vwControls") }} c
        join {{ ref("vwTenantAuthorityTenant") }} tat on c.Controls_TenantId = tat.TenantAuthority_TenantId
        where c.Controls_IsCurrent = 1
    ),
    pd as (
        select pd.*, tat.tgt_TenantId, tat.tgt_AuthorityId
        from {{ ref("vwPolicyDomain") }} pd
        join {{ ref("vwTenantAuthorityTenant") }} tat on pd.PolicyDomain_TenantId = tat.TenantAuthority_TenantId
    ),
    p as (
        select p.*, tat.tgt_TenantId, tat.tgt_AuthorityId
        from {{ ref("vwPolicy") }} p
        join {{ ref("vwTenantAuthorityTenant") }} tat on p.Policy_TenantId = tat.TenantAuthority_TenantId
        where p.Policy_IsTemplate = 0 and p.Policy_IsCurrent = 1
    ),
    q1 as (  -- ap + custom - OK
        select
            ap.Authority_TenantId,
            ap.Authority_Id,
            ap.Authority_Name,
            ap.AuthorityProvision_Id,
            ap.AuthorityProvision_ReferenceId,
            ap.AuthorityProvision_Name,
            custom.CustomFieldName,
            custom.CustomValue,
            '' as eol
        from ap
        join custom on ap.AuthorityProvision_Id = custom.AuthorityProvision_Id
    -- where 1=1
    -- and ap.Authority_Id = 63
    ),
    q2 as (  -- ap + custom + pc
        select
            ap.Authority_TenantId,
            ap.Authority_Id,
            ap.Authority_Name,
            ap.AuthorityProvision_Id,
            ap.AuthorityProvision_ReferenceId,
            ap.AuthorityProvision_Name,
            custom.CustomFieldName,
            custom.CustomValue,
            pc.tgt_TenantId ProvisionControl_TenantId,
            pc.ProvisionControl_Id
        from ap
        join custom on ap.AuthorityProvision_Id = custom.AuthorityProvision_Id
        left join
            pc
            on pc.ProvisionControl_AuthorityReferenceId = ap.AuthorityProvision_Id
            and pc.tgt_AuthorityId = ap.Authority_Id
    -- where 1=1
    -- and ap.Authority_Id = 63
    ),
    q3 as (  -- ap + custom + pc + c
        select
            ap.Authority_TenantId,
            ap.Authority_Id,
            ap.Authority_Name,
            ap.AuthorityProvision_Id,
            ap.AuthorityProvision_ReferenceId,
            ap.AuthorityProvision_Name,
            custom.CustomFieldName,
            custom.CustomValue,
            pc.tgt_TenantId ProvisionControl_TenantId,
            pc.ProvisionControl_Id,
            c.tgt_TenantId Controls_TenantId,
            c.Controls_Id,
            c.Controls_IsCurrent
        from ap
        join custom on ap.AuthorityProvision_Id = custom.AuthorityProvision_Id
        left join
            pc
            on pc.ProvisionControl_AuthorityReferenceId = ap.AuthorityProvision_Id
            and pc.tgt_AuthorityId = ap.Authority_Id
        left join c on c.Controls_Id = pc.ProvisionControl_ControlsId and c.tgt_AuthorityId = pc.tgt_AuthorityId
    -- where 1=1
    -- and ap.Authority_Id = 63
    ),
    q4 as (  -- ap + custom + pc + c + pd
        select
            ap.Authority_TenantId,
            ap.Authority_Id,
            ap.Authority_Name,
            ap.AuthorityProvision_Id,
            ap.AuthorityProvision_ReferenceId,
            ap.AuthorityProvision_Name,
            custom.CustomFieldName,
            custom.CustomValue,
            pc.tgt_TenantId ProvisionControl_TenantId,
            pc.ProvisionControl_Id,
            c.tgt_TenantId Controls_TenantId,
            c.Controls_Id,
            c.Controls_IsCurrent,
            pd.tgt_TenantId PolicyDomain_TenantId,
            pd.PolicyDomain_Id
        from ap
        join custom on ap.AuthorityProvision_Id = custom.AuthorityProvision_Id
        left join
            pc
            on pc.ProvisionControl_AuthorityReferenceId = ap.AuthorityProvision_Id
            and pc.tgt_AuthorityId = ap.Authority_Id
        left join c on c.Controls_Id = pc.ProvisionControl_ControlsId and c.tgt_AuthorityId = pc.tgt_AuthorityId
        left join pd on pd.PolicyDomain_Id = c.Controls_PolicyDomainId and pd.tgt_AuthorityId = c.tgt_AuthorityId
    -- where 1=1
    -- and ap.Authority_Id = 63
    ),
    q5 as (  -- ap + custom + pc + c + pd + p
        select
            ap.Authority_TenantId,
            ap.Authority_Id,
            ap.Authority_Name,
            ap.AuthorityProvision_Id,
            ap.AuthorityProvision_ReferenceId,
            ap.AuthorityProvision_Name,
            ap.AuthorityProvision_Order,
            custom.CustomFieldName,
            custom.CustomValue,
            pc.tgt_TenantId ProvisionControl_TenantId,
            pc.ProvisionControl_Id,
            c.tgt_TenantId Controls_TenantId,
            c.Controls_Id,
            c.Controls_Reference,
            c.Controls_IsCurrent,
            c.Controls_Order,
            c.Controls_TemplateControlId,
            c.Controls_RiskStatus,
            pd.tgt_TenantId PolicyDomain_TenantId,
            pd.PolicyDomain_Id,
            pd.PolicyDomain_Name,
            p.tgt_TenantId Policy_TenantId,
            p.Policy_Id,
            p.Policy_Name,
            p.Policy_Description,
            p.Policy_StatusCode,
            p.Policy_Type,
            p.Policy_IsTemplate,
            p.Policy_Version,
            p.Policy_IsCurrent
        from ap
        join custom on ap.AuthorityProvision_Id = custom.AuthorityProvision_Id
        left join
            pc
            on pc.ProvisionControl_AuthorityReferenceId = ap.AuthorityProvision_Id
            and pc.tgt_AuthorityId = ap.Authority_Id
        left join c on c.Controls_Id = pc.ProvisionControl_ControlsId and c.tgt_AuthorityId = pc.tgt_AuthorityId
        left join pd on pd.PolicyDomain_Id = c.Controls_PolicyDomainId and pd.tgt_AuthorityId = c.tgt_AuthorityId
        left join p on p.Policy_Id = pd.PolicyDomain_PolicyId and p.tgt_AuthorityId = pd.tgt_AuthorityId
    -- where 1=1
    -- and ap.Authority_Id = 63
    ),
    base as (
        -- Provisions with no controls
        select *
        from q5
        where 1 = 1 and ProvisionControl_Id is null  -- 256 rows (56 rows others)
        union all
        -- Provisions with controls
        -- Target 56 rows - got 5 extra rows
        -- duplicate 17396, 17400, 17401, 17402, 17403 - suspect at controls
        select *
        from q5
        where 1 = 1 and ProvisionControl_Id is not null and controls_Id is not NULL and policy_id is not null
    )

select distinct
    Authority_TenantId TenantId,
    Authority_Id,
    Authority_Name,
    AuthorityProvision_ReferenceId ProvisionId,
    AuthorityProvision_Name ProvisionName,
    CustomFieldName,
    CustomValue,
    Controls_Reference ControlId,
    PolicyDomain_Name DomainName,
    Policy_Name ControlSetName,
    Policy_Description ControlSetDescription
from
    base
    -- where AuthorityProvision_Id in (17396, 17400, 17401, 17402, 17403)
    -- where Authority_Id = 63
    

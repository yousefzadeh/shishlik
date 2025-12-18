{# 

Controls
One row is a relation Controlset, Domain, Control
PK = Controls_Id
 #}
with
    base as (
        select
            Controls_Id,  -- PK
            -- Level 1
            Controlset_Id,
            Controlset_TenantId Tenant_Id,
            Controlset_Name,
            Controlset_Description,
            Controlset_Tags,
            Controlset_SupplierName,
            Controlset_Status,
            Controlset_StatusCode,
            Controlset_Type,
            Controlset_LastReviewDate,
            Controlset_NextReviewDate,
            Controlset_IsTemplate,
            Controlset_TemplatedId,
            Controlset_ParentControlsetId,
            Controlset_RootControlsetId,
            Controlset_Version,
            Controlset_VersionDate,
            Controlset_PublishedDate,
            Controlset_PublishedById,
            Controlset_ImageUrl,
            Controlset_HideResponsibilityTasksUntilRepublished,
            Controlset_LastPublishedDate,
            Controlset_IsCurrent,
            -- Level 2
            ControlsetDomain_Id,
            ControlsetDomain_Name,
            ControlsetDomain_Custom,
            -- Level 3
            Controls_Name,
            Controls_Detail,
            Controls_Tags,
            Controls_Order,
            Controls_PolicyDomainId,
            Controls_TemplateControlId,
            Controls_TenantId,
            Controls_RiskStatus,
            Controls_Reference,
            Controls_ParentControlId,
            Controls_RootControlId,
            Controls_IsCurrent
        from {{ ref("vwControlset") }} cs
        join {{ ref("vwControls") }} c on cs.ControlsetDomain_Id = c.Controls_PolicyDomainId
    ),
    zero as (
        select
            0 Controls_Id,
            0 Controlset_Id,
            0 Controlset_TenantId,
            'Unassigned Control Set' Controlset_Name,
            '' Controlset_Description,
            NULL Controlset_Tags,
            NULL Controlset_SupplierName,
            NULL Controlset_Status,
            NULL Controlset_StatusCode,
            NULL Controlset_Type,
            NULL Controlset_LastReviewDate,
            NULL Controlset_NextReviewDate,
            NULL Controlset_IsTemplate,
            0 Controlset_TemplatedId,
            0 Controlset_ParentControlsetId,
            0 Controlset_RootControlsetId,
            1 Controlset_Version,
            NULL Controlset_VersionDate,
            NULL Controlset_PublishedDate,
            NULL Controlset_PublishedById,
            NULL Controlset_ImageUrl,
            NULL Controlset_HideResponsibilityTasksUntilRepublished,
            NULL Controlset_LastPublishedDate,
            1 Controlset_IsCurrent,
            0 ControlsetDomain_Id,
            'Unassigned Domain' ControlsetDomain_Name,
            NULL ControlsetDomain_Custom,
            'Unassigned Control' Controls_Name,
            NULL Controls_Detail,
            NULL Controls_Tags,
            NULL Controls_Order,
            0 Controls_PolicyDomainId,
            0 Controls_TemplateControlId,
            0 Controls_TenantId,
            NULL Controls_RiskStatus,
            NULL Controls_Reference,
            0 Controls_ParentControlId,
            0 Controls_RootControlId,
            1 Controls_IsCurrent
    )

select *
from base
union all
select *
from zero

{# 
    Control Set Dimension
    One row is a relation Controlset
    PK = Controlset_Id
    PK value 0 is the unassigned control set
 #}
with
    base as (
        select distinct
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
            Controlset_ParentControlsetId ControlSet_ParentId,
            Controlset_RootControlsetId ControlSet_RootId,
            Controlset_Version,
            Controlset_VersionDate,
            Controlset_PublishedDate,
            Controlset_PublishedById,
            Controlset_ImageUrl,
            Controlset_HideResponsibilityTasksUntilRepublished,
            Controlset_LastPublishedDate,
            Controlset_IsCurrent
        from {{ ref("vwControlset") }}
    ),
    zero as (
        select
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
            0 Controlset_ParentId,
            0 Controlset_RootId,
            1 Controlset_Version,
            NULL Controlset_PublishedDate,
            NULL Controlset_VersionDate,
            NULL Controlset_PublishedById,
            NULL Controlset_ImageUrl,
            NULL Controlset_HideResponsibilityTasksUntilRepublished,
            NULL Controlset_LastPublishedDate,
            1 Controlset_IsCurrent
    ),
    un as (
        select *
        from base
        union all
        select *
        from zero
    )

select child.*, parent.Controlset_Name ParentControlset_Name
from un child
left join un parent on parent.Controlset_Id = child.Controlset_ParentId

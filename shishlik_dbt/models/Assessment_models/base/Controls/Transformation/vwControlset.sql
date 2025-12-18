{{ config(materialized="view") }}
with
    ctrlset_label as (
select distinct
    p.Policy_TenantId Tenant_Id,
    p.Policy_Id,
    case when tpc.ThirdPartyControl_Label = 'Type' then
    tpa.ThirdPartyAttributes_Label end Label
from {{ ref("vwPolicy") }} p
left join {{ ref("vwPolicyCustomAttributeData") }} pcad on pcad.PolicyCustomAttributeData_PolicyId = p.Policy_Id
left join
    {{ ref("vwThirdPartyAttributes") }} tpa
    on tpa.ThirdPartyAttributes_Id = pcad.PolicyCustomAttributeData_ThirdPartyAttributesId
left join
    {{ ref("vwThirdPartyControl") }} tpc on tpc.ThirdPartyControl_Id = tpa.ThirdPartyAttributes_ThirdPartyControlId
where tpc.ThirdPartyControl_Type = 1
    ),
    ctrlset as (
        select
            {{ system_fields_macro() }},
            [tenantid],
            [name],
            cast([description] as nvarchar(4000)) description,
            cast([tags] as nvarchar(4000)) tags,
            cast([suppliername] as nvarchar(4000)) suppliername,
            [status],
            case
                when [status] = 1
                then 'Edit'
                when [status] = 2
                then 'Published'
                when [status] = 100
                then 'Deprecated'
                else 'Undefined'
            end as [statuscode],
            cast([type] as nvarchar(4000))[type],
            [lastreviewdate],
            [nextreviewdate],
            [istemplate],
            [templatedid],
            [parentpolicyid] parentcontrolsetid,
            coalesce([rootpolicyid], [id]) rootcontrolsetid,
            [version],
            'v'
            + cast([version] as varchar(13))
            + ' ('
            + coalesce(
                cast(format([publisheddate], 'dd MMM, yyyy') as varchar),
                cast(format(getdate(), 'dd MMM, yyyy') as varchar)
            )
            + ')' as versiondate,
            [publisheddate],
            [publishedbyid],
            cast([imageurl] as nvarchar(4000)) imageurl,
            [hideresponsibilitytasksuntilrepublished],
            [lastpublisheddate],
            {{ IsCurrentRow("RootPolicyId") }}
        from {{ source("assessment_models", "Policy") }} {{ system_remove_IsDeleted() }}
    ),
    ctrlset_domain as (
        select
            {{ system_fields_macro() }},
            cast(name as nvarchar(4000)) name,
            cast(custom as nvarchar(4000)) custom,
            policyid controlsetid,
            controlsid,
            tenantid
        from {{ source("assessment_models", "PolicyDomain") }}
    ),
    c as (
        select
            {{ col_rename("Id", "Controlset") }},
            {{ col_rename("TenantId", "Controlset") }},
            ctrlset_label.Label as ControlSet_Label,
            {{ col_rename("Name", "Controlset") }},
            {{ col_rename("Description", "Controlset") }},

            {{ col_rename("CreationTime", "Controlset") }},
            coalesce(
                LastModificationTime, CreationTime
            ) as ControlSet_LastModificationTime,
            {{ col_rename("Tags", "Controlset") }},
            {{ col_rename("SupplierName", "Controlset") }},
            {{ col_rename("Status", "Controlset") }},
            {{ col_rename("StatusCode", "Controlset") }},
            {{ col_rename("Type", "Controlset") }},

            {{ col_rename("LastReviewDate", "Controlset") }},
            {{ col_rename("NextReviewDate", "Controlset") }},
            {{ col_rename("IsTemplate", "Controlset") }},
            {{ col_rename("TemplatedId", "Controlset") }},
            {{ col_rename("VersionDate", "Controlset") }},

            {{ col_rename("ParentControlsetId", "Controlset") }},
            {{ col_rename("RootControlsetId", "Controlset") }},
            {{ col_rename("Version", "Controlset") }},
            {{ col_rename("PublishedDate", "Controlset") }},

            {{ col_rename("PublishedById", "Controlset") }},
            coalesce(u.AbpUsers_FullName, 'Unspecified') as ControlSet_PublishedByUser,

            {{ col_rename("ImageUrl", "Controlset") }},
            {{ col_rename("HideResponsibilityTasksUntilRepublished", "Controlset") }},
            {{ col_rename("LastPublishedDate", "Controlset") }},
            {{ col_rename("IsCurrent", "Controlset") }}
        from ctrlset
        left join ctrlset_label on ctrlset_label.Tenant_Id = ctrlset.TenantId and ctrlset_label.Policy_Id = Id
        left join {{ ref("vwAbpUser") }} u on u.AbpUsers_id = ctrlset.publishedbyid
    ),
    cd as (
        select
            {{ col_rename("Id", "ControlsetDomain") }},
            {{ col_rename("ControlsetId", "ControlsetDomain") }},
            {{ col_rename("ControlsId", "ControlsetDomain") }},
            {{ col_rename("Name", "ControlsetDomain") }},
            {{ col_rename("Custom", "ControlsetDomain") }}
        from ctrlset_domain
    )

select *
from c
join cd on cd.controlsetdomain_controlsetid = c.controlset_id

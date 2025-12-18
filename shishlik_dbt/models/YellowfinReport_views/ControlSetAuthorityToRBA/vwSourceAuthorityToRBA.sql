with
    Authority as (select Id Authority_Id, Name Authority_Name from {{ source("assessment_models", "Authority") }}),
    AuthorityProvision as (
        select
            Id AuthorityProvision_Id,
            AuthorityId AuthorityProvision_AuthorityId,
            ReferenceId AuthorityProvision_ReferenceId,
            Name AuthorityProvision_Name
        from {{ source("assessment_models", "AuthorityProvision") }}
    ),
    Assessment_with_template as (
        select
            ass.ID Assessment_Id,
            ass.NameVarchar Assessment_Name,
            tpl.NameVarchar + case
                when tpl.IsDeleted = 1 then ' (Deleted)' when tpl.IsArchived = 1 then ' (Archived)' else ''
            end Template_Name,
            ass.AuthorityId Assessment_AuthorityId,
            ass.TenantId Assessment_TenantId
        from {{ source("assessment_models", "Assessment") }} as ass
        join {{ source("assessment_models", "Assessment") }} as tpl on ass.CreatedFromTemplateId = tpl.ID
        where
            ass.IsTemplate = 0
            and ass.IsDeleted = 0
            and ass.IsArchived = 0
            and ass.IsDeprecatedAssessmentVersion = 0
            and ass.Status in (4, 5, 6)
            and ass.WorkFlowId = 1
            and ass.AuthorityId is not NULL
    ),
    Assessment_without_template as (
        select
            ass.ID Assessment_Id,
            ass.NameVarchar Assessment_Name,
            'No Template' Template_Name,
            ass.AuthorityId Assessment_AuthorityId,
            ass.TenantId Assessment_TenantId
        from {{ source("assessment_models", "Assessment") }} as ass
        where
            ass.IsTemplate = 0
            and ass.IsDeleted = 0
            and ass.IsArchived = 0
            and ass.IsDeprecatedAssessmentVersion = 0
            and ass.Status in (4, 5, 6)
            and ass.WorkFlowId = 1
            and ass.CreatedFromTemplateId is NULL
            and ass.AuthorityId is not NULL
    ),
    Assessment as (
        select *
        from Assessment_with_template
        union all
        select *
        from Assessment_without_template
    ),
    AssessmentDomain as (
        select Id AssessmentDomain_Id, AssessmentId AssessmentDomain_AssessmentId, Name AssessmentDomain_Name
        from {{ source("assessment_models", "AssessmentDomain") }}
    ),
    AssessmentResponse as (
        select Id AssessmentResponse_Id, AssessmentId AssessmentResponse_AssessmentId
        from {{ source("assessment_models", "AssessmentResponse") }}
    ),
    AssessmentDomainProvision_selected as (
        select
            Id AssessmentDomainProvision_Id,
            AuthorityProvisionId AssessmentDomainProvision_AuthorityProvisionId,
            AssessmentDomainId AssessmentDomainProvision_AssessmentDomainId,
            'Selected' AssessmentDomainProvision_part
        from {{ source("assessment_models", "AssessmentDomainProvision") }}
        where IsDeleted = 0
    ),
    AssessmentDomainProvision_excluded as (
        select
            Id AssessmentDomainProvision_Id,
            AuthorityProvisionId AssessmentDomainProvision_AuthorityProvisionId,
            AssessmentDomainId AssessmentDomainProvision_AssessmentDomainId,
            'Excluded' AssessmentDomainProvision_part
        from {{ source("assessment_models", "AssessmentDomainProvision") }}
        where IsDeleted = 1
    ),
    AssessmentCustomField as (
        select AssessmentId AssessmentCustomField_AssessmentId, CustomFieldId AssessmentCustomField_CustomFieldId
        from {{ source("assessment_models", "AssessmentCustomField") }}
    ),
    CustomField as (
        select Id CustomField_Id, Name CustomField_Name from {{ source("assessment_models", "CustomField") }}
    ),
    ResponseData as (
        -- For response data, we need to join with CustomFieldAttribute to get the name of the custom field
        select
            adprd.Id AssessmentDomainProvisionResponseData_Id,
            adprd.AssessmentResponseId AssessmentDomainProvisionResponseData_AssessmentResponseId,
            adprd.AssessmentDomainProvisionId AssessmentDomainProvisionResponseData_AssessmentDomainProvisionId,
            COALESCE(
                cfa.AttributeName, adprd.CustomFieldText
            ) AssessmentDomainProvisionResponseData_CustomFieldResponse,
            adprd.CustomFieldId AssessmentDomainProvisionResponseData_CustomFieldId
        from {{ source("assessment_models", "AssessmentDomainProvisionResponseData") }} adprd
        left join
            {{ source("assessment_models", "CustomFieldAttribute") }} cfa
            on cfa. [TenantId] = adprd. [TenantId]
            and cfa. [Id] = adprd. [CustomFieldAttributeId]
            and cfa. [IsDeleted] = 0
        where adprd.IsDeleted = 0
    ),
    prov_custom as (
        select
            Tenant_Id,
            authorityprovisioncustomvalue_authorityid,
            authorityprovisioncustomvalue_authorityprovisionid,
            authorityprovisioncustomvalue_fieldname,
            authorityprovisioncustomvalue_value,
            authorityprovisioncustomvalue_fieldtype
        from {{ ref("vwAuthorityProvisionCustomValue") }}
    ),
    -- --
    selected_requirement_responded as (
        select
            'Selected requirement and responded' part,
            -- Filter
            ass.Assessment_TenantId filter_TenantId,
            auth.Authority_Name filter_Source_Authority,
            ass.Template_Name filter_Template_Name,
            ass.Assessment_Name filter_Assessment_Name,
            cf.CustomField_Name filter_Assessment_Field_Name,
            -- Table
            ass.Assessment_Name Assessment_Name,
            ad.AssessmentDomain_Name AssessmentDomain_Name,
            ap.AuthorityProvision_ReferenceId Provision_IDRef,
            ap.AuthorityProvision_Name Provision_Name,
            resp.AssessmentDomainProvisionResponseData_CustomFieldResponse Actual_Response,
            resp.AssessmentDomainProvisionResponseData_ID Response_Id,
            -- Internal use
            ap.AuthorityProvision_Id,
            ar.AssessmentResponse_Id
        --
        -- Table Joins
        --
        -- Authority 
        from Authority as auth
        -- Provision to Provision Custom Field Value
        inner join AuthorityProvision as ap on auth.Authority_Id = ap.AuthorityProvision_AuthorityId
        -- Assessment Template 
        inner join Assessment as ass on ass.Assessment_AuthorityId = auth.Authority_Id
        -- Assessment Custom Field Value
        inner join AssessmentCustomField as acf on ass.Assessment_ID = acf.AssessmentCustomField_AssessmentId
        inner join CustomField as cf on acf.AssessmentCustomField_CustomFieldId = cf.CustomField_ID
        -- Assessment Domain Provision Response Data
        inner join AssessmentDomain as ad on ass.Assessment_ID = ad.AssessmentDomain_AssessmentId
        -- Selected Requirements
        inner join
            AssessmentDomainProvision_Selected as req
            on ap.AuthorityProvision_Id = req.AssessmentDomainProvision_AuthorityProvisionId
            and ad.AssessmentDomain_ID = req.AssessmentDomainProvision_AssessmentDomainId
        -- Responded - Responses
        inner join AssessmentResponse as ar on ass.Assessment_ID = ar.AssessmentResponse_AssessmentId
        inner hash join
            ResponseData as resp
            on ar.AssessmentResponse_ID = resp.AssessmentDomainProvisionResponseData_AssessmentResponseId
            and req.AssessmentDomainProvision_Id
            = resp.AssessmentDomainProvisionResponseData_AssessmentDomainProvisionId
            and resp.AssessmentDomainProvisionResponseData_CustomFieldId = cf.CustomField_ID
    ),
    selected_requirement_not_responded as (
        select
            'Selected requirement but not responded' part,
            -- Filter
            ass.Assessment_TenantId filter_TenantId,
            auth.Authority_Name filter_Source_Authority,
            ass.Template_Name filter_Template_Name,
            ass.Assessment_Name filter_Assessment_Name,
            cf.CustomField_Name filter_Assessment_Field_Name,
            -- Table
            ass.Assessment_Name Assessment_Name,
            ad.AssessmentDomain_Name AssessmentDomain_Name,
            ap.AuthorityProvision_ReferenceId Provision_IDRef,
            ap.AuthorityProvision_Name Provision_Name,
            'No Response' Actual_Response,
            AssessmentDomainProvision_Id Response_Id,
            -- Internal use
            ap.AuthorityProvision_Id,
            0 AssessmentResponse_Id
        --
        -- Table Joins
        --
        -- Authority 
        from Authority as auth
        -- Provision to Provision Custom Field Value
        inner join AuthorityProvision as ap on auth.Authority_Id = ap.AuthorityProvision_AuthorityId
        -- Assessment Template 
        inner join Assessment as ass on ass.Assessment_AuthorityId = auth.Authority_Id
        -- Assessment Custom Field Value
        inner join AssessmentCustomField as acf on ass.Assessment_ID = acf.AssessmentCustomField_AssessmentId
        inner join CustomField as cf on acf.AssessmentCustomField_CustomFieldId = cf.CustomField_ID
        -- Assessment Domain Provision Response Data
        inner join AssessmentDomain as ad on ass.Assessment_ID = ad.AssessmentDomain_AssessmentId
        inner join
            AssessmentDomainProvision_Selected as req
            on ap.AuthorityProvision_Id = req.AssessmentDomainProvision_AuthorityProvisionId
            and ad.AssessmentDomain_ID = req.AssessmentDomainProvision_AssessmentDomainId
        -- Except responded 
        where
            not exists (
                select 1
                from selected_requirement_responded srr
                where srr.AuthorityProvision_Id = ap.AuthorityProvision_Id
            )
    ),
    excluded_requirement as (
        select
            'Excluded requirement' part,
            -- Filter
            ass.Assessment_TenantId filter_TenantId,
            auth.Authority_Name filter_Source_Authority,
            ass.Template_Name filter_Template_Name,
            ass.Assessment_Name filter_Assessment_Name,
            cf.CustomField_Name filter_Assessment_Field_Name,
            -- Table
            ass.Assessment_Name Assessment_Name,
            ad.AssessmentDomain_Name AssessmentDomain_Name,
            ap.AuthorityProvision_ReferenceId Provision_IDRef,
            ap.AuthorityProvision_Name Provision_Name,
            'Requirement excluded from assessment' Actual_Response,
            req.AssessmentDomainProvision_Id Response_Id,  -- Unique ID of requirement
            -- Internal use
            ap.AuthorityProvision_Id,
            0 AssessmentResponse_Id
        --
        -- Table Joins
        --
        -- Authority 
        from Authority as auth
        -- Provision to Provision Custom Field Value
        inner join AuthorityProvision as ap on auth.Authority_Id = ap.AuthorityProvision_AuthorityId
        -- Assessment Template 
        inner join Assessment as ass on ass.Assessment_AuthorityId = auth.Authority_Id
        -- Assessment Custom Field Value
        inner join AssessmentCustomField as acf on ass.Assessment_ID = acf.AssessmentCustomField_AssessmentId
        inner join CustomField as cf on acf.AssessmentCustomField_CustomFieldId = cf.CustomField_ID
        -- Assessment Domain Provision Response Data
        inner join AssessmentDomain as ad on ass.Assessment_ID = ad.AssessmentDomain_AssessmentId
        -- Excluded Requirements
        inner join
            AssessmentDomainProvision_excluded as req
            on ap.AuthorityProvision_Id = req.AssessmentDomainProvision_AuthorityProvisionId
            and ad.AssessmentDomain_ID = req.AssessmentDomainProvision_AssessmentDomainId
    ),
    -- -----
    uni as (
        select *
        from selected_requirement_responded
        union all
        select *
        from selected_requirement_not_responded
        union all
        select *
        from excluded_requirement
    ),
    final as (
        select 
        uni.*,
        -- Section header
        prov_custom.AuthorityProvisionCustomValue_FieldName section_Authority_Field_Name,
        case
            when
                prov_custom.AuthorityProvisionCustomValue_Value is NULL
                or prov_custom.AuthorityProvisionCustomValue_Value = ''
            then 'Unassigned'
            else prov_custom.AuthorityProvisionCustomValue_Value
        end as Authority_Field_Value, 
        prov_custom.AuthorityProvisionCustomValue_FieldType CustomFieldType
        from uni 
        join prov_custom on uni.AuthorityProvision_Id = prov_custom.AuthorityProvisionCustomValue_AuthorityProvisionId
            and uni.filter_TenantId = prov_custom.Tenant_Id
    )
select *
from final
 -- Test
    -- where filter_TenantId = 1384
    -- and CustomFieldType = 1  


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
    ControlSet as (
        select pol.Id ControlSet_Id, pol.Name ControlSet_Name, pd.Id ControlsDomain_Id, pd.Name ControlsDomain_Name
        from {{ source("assessment_models", "Policy") }} pol
        join {{ source("assessment_models", "PolicyDomain") }} pd on pol.Id = pd.PolicyId
    ),
    Controls as (
        select Id Controls_Id, PolicyDomainId Controls_DomainId, Reference Controls_ReferenceId, Name Controls_Name
        from {{ source("assessment_models", "Controls") }}
    ),
    Assessment_with_template as (
        select
            ass.ID Assessment_Id,
            ass.NameVarchar Assessment_Name,
            tpl.NameVarchar + case
                when tpl.IsDeleted = 1 then ' (Deleted)' when tpl.IsArchived = 1 then ' (Archived)' else ''
            end Template_Name,
            ass.PolicyId Assessment_ControlSetId,
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
            and ass.PolicyId is not NULL
    ),
    Assessment_without_template as (
        select
            ass.ID Assessment_Id,
            ass.NameVarchar Assessment_Name,
            'No Template' Template_Name,
            ass.PolicyId Assessment_ControlSetId,
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
            and ass.PolicyId is not NULL
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
    AssessmentDomainControl_selected as (
        select
            Id AssessmentDomainControl_Id,
            ControlsId AssessmentDomainControl_ControlsId,
            AssessmentDomainId AssessmentDomainControl_AssessmentDomainId,
            'Selected' AssessmentDomainControl_part
        from {{ source("assessment_models", "AssessmentDomainControl") }}
        where IsDeleted = 0
    ),
    AssessmentDomainControl_excluded as (
        select
            Id AssessmentDomainControl_Id,
            ControlsId AssessmentDomainControl_ControlsId,
            AssessmentDomainId AssessmentDomainControl_AssessmentDomainId,
            'Excluded' AssessmentDomainControl_part
        from {{ source("assessment_models", "AssessmentDomainControl") }}
        where IsDeleted = 1
    ),
    AssessmentCustomField as (
        select AssessmentId AssessmentCustomField_AssessmentId, CustomFieldId AssessmentCustomField_CustomFieldId
        from {{ source("assessment_models", "AssessmentCustomField") }}
    ),
    CustomField as (
        select Id CustomField_Id, Name CustomField_Name from {{ source("assessment_models", "CustomField") }}
    ),
    AssessmentDomainControlResponseData as (
        select
            AssessmentDomainControlResponseData_Id,
            AssessmentDomainControlResponseData_AssessmentResponseId,
            AssessmentDomainControlResponseData_AssessmentDomainControlId,
            AssessmentDomainControlResponseData_CustomFieldResponse,
            AssessmentDomainControlResponseData_CustomFieldId,
            1 AssessmentDomainControlResponseData_IsResponded,
            0 AssessmentDomainControlResponseData_IsDeleted
        from {{ ref("vwAssessmentDomainControlResponseData") }}
    ),
    prov_custom as (
        select
            AuthorityProvisionCustomValue_AuthorityProvisionId,
            AuthorityProvisionCustomValue_FieldName,
            AuthorityProvisionCustomValue_Value
        from {{ ref("vwAuthorityProvisionCustomValue") }}
    ),
    -- --
    selected_requirement_responded as (
        select
            'Selected requirement and responded' part,
            -- Filter
            ass.Assessment_TenantId filter_TenantId,
            cs.ControlSet_Name filter_Source_ControlSet,
            ass.Template_Name filter_Template_Name,
            ass.Assessment_Name filter_Assessment_Name,
            cf.CustomField_Name filter_Assessment_Field_Name,
            -- Table
            ass.Assessment_Name Assessment_Name,
            cs.ControlsDomain_Name,
            c.Controls_ReferenceId Controls_IDRef,
            c.Controls_Name Controls_Name,
            cf.CustomField_Name,
            resp.AssessmentDomainControlResponseData_CustomFieldResponse Actual_Response,
            resp.AssessmentDomainControlResponseData_ID Response_Id,
            -- Internal use
            c.Controls_Id,
            ar.AssessmentResponse_Id
        --
        -- Table Joins
        --
        -- ControlSet 
        from ControlSet as cs
        -- Controls Domain
        inner join Controls as c on cs.ControlsDomain_Id = c.Controls_DomainId
        -- Assessment Template 
        inner join Assessment as ass on ass.Assessment_ControlSetId = cs.ControlSet_Id
        -- Assessment Custom Field Value
        inner join AssessmentCustomField as acf on ass.Assessment_ID = acf.AssessmentCustomField_AssessmentId
        inner join CustomField as cf on acf.AssessmentCustomField_CustomFieldId = cf.CustomField_ID
        -- Assessment Domain Provision Response Data
        inner join AssessmentDomain as ad on ass.Assessment_ID = ad.AssessmentDomain_AssessmentId
        inner join
            AssessmentDomainControl_Selected as req
            on c.Controls_Id = req.AssessmentDomainControl_ControlsId
            and ad.AssessmentDomain_ID = req.AssessmentDomainControl_AssessmentDomainId
        inner join AssessmentResponse as ar on ass.Assessment_ID = ar.AssessmentResponse_AssessmentId
        inner join
            AssessmentDomainControlResponseData as resp
            on ar.AssessmentResponse_ID = resp.AssessmentDomainControlResponseData_AssessmentResponseId
            and req.AssessmentDomainControl_Id = resp.AssessmentDomainControlResponseData_AssessmentDomainControlId
            and resp.AssessmentDomainControlResponseData_CustomFieldId = cf.CustomField_ID
    ),
    selected_requirement_not_responded as (
        select
            'Selected requirement but not responded' part,
            -- Filter
            ass.Assessment_TenantId filter_TenantId,
            cs.ControlSet_Name filter_Source_ControlSet,
            ass.Template_Name filter_Template_Name,
            ass.Assessment_Name filter_Assessment_Name,
            cf.CustomField_Name filter_Assessment_Field_Name,
            -- Table
            ass.Assessment_Name Assessment_Name,
            cs.ControlsDomain_Name,
            c.Controls_ReferenceId Controls_IDRef,
            c.Controls_Name Controls_Name,
            cf.CustomField_Name,
            'No Response' Actual_Response,
            AssessmentDomainControl_Id Response_Id,
            -- Internal use
            c.Controls_Id,
            0 AssessmentResponse_Id
        --
        -- Table Joins
        --
        -- ControlSet 
        from ControlSet as cs
        -- Controls Domain
        inner join Controls as c on cs.ControlsDomain_Id = c.Controls_DomainId
        -- Assessment Template 
        inner join Assessment as ass on ass.Assessment_ControlSetId = cs.ControlSet_Id
        -- Assessment Custom Field Value
        inner join AssessmentCustomField as acf on ass.Assessment_ID = acf.AssessmentCustomField_AssessmentId
        inner join CustomField as cf on acf.AssessmentCustomField_CustomFieldId = cf.CustomField_ID
        -- Assessment Domain Provision Response Data
        inner join AssessmentDomain as ad on ass.Assessment_ID = ad.AssessmentDomain_AssessmentId
        inner join
            AssessmentDomainControl_Selected as req
            on c.Controls_Id = req.AssessmentDomainControl_ControlsId
            and ad.AssessmentDomain_ID = req.AssessmentDomainControl_AssessmentDomainId
        where not exists (select 1 from selected_requirement_responded srr where srr.Controls_Id = c.Controls_Id)
    ),
    excluded_requirement as (
        select
            'Excluded Requirement' part,
            -- Filter
            ass.Assessment_TenantId filter_TenantId,
            cs.ControlSet_Name filter_Source_ControlSet,
            ass.Template_Name filter_Template_Name,
            ass.Assessment_Name filter_Assessment_Name,
            cf.CustomField_Name filter_Assessment_Field_Name,
            -- Table
            ass.Assessment_Name Assessment_Name,
            cs.ControlsDomain_Name,
            c.Controls_ReferenceId Controls_IDRef,
            c.Controls_Name Controls_Name,
            cf.CustomField_Name,
            'Requirement excluded from assessment' Actual_Response,
            AssessmentDomainControl_Id Response_Id,
            -- Internal use
            c.Controls_Id,
            0 AssessmentResponse_Id
        --
        -- Table Joins
        --
        -- ControlSet 
        from ControlSet as cs
        -- Controls Domain
        inner join Controls as c on cs.ControlsDomain_Id = c.Controls_DomainId
        -- Assessment Template 
        inner join Assessment as ass on ass.Assessment_ControlSetId = cs.ControlSet_Id
        -- Assessment Custom Field Value
        inner join AssessmentCustomField as acf on ass.Assessment_ID = acf.AssessmentCustomField_AssessmentId
        inner join CustomField as cf on acf.AssessmentCustomField_CustomFieldId = cf.CustomField_ID
        -- Assessment Domain Provision Response Data
        inner join AssessmentDomain as ad on ass.Assessment_ID = ad.AssessmentDomain_AssessmentId
        inner join
            AssessmentDomainControl_excluded as req
            on c.Controls_Id = req.AssessmentDomainControl_ControlsId
            and ad.AssessmentDomain_ID = req.AssessmentDomainControl_AssessmentDomainId
    ),
    -- -----
    final as (
        select *
        from selected_requirement_responded
        union all
        select *
        from selected_requirement_not_responded
        union all
        select *
        from excluded_requirement
    )
select *
from final

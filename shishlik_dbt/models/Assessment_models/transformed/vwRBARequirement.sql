with
    auth_based as (
        -- Assessment -> AssessmentDomain -> AssessmentDomainProvision -> AuthorityProvision -> ReferenceId column
        select
            AssessmentDomainProvision_AssessmentDomainId,
            AssessmentDomainProvision_Id,
            AssessmentDomainProvision_TenantId,
            AuthorityProvision_ReferenceId,
            AuthorityProvision_Name
        from {{ ref("vwAssessmentDomainProvision") }} adp
        join
            {{ ref("vwAuthorityProvision") }} ap
            on ap.AuthorityProvision_Id = adp.AssessmentDomainProvision_AuthorityProvisionId
    ),
    control_based as (
        -- Assessment -> AssessmentDomain -> AssessmentDomainControl-> Controls -> Reference column
        select
            AssessmentDomainControl_AssessmentDomainId,
            AssessmentDomainControl_Id,
            AssessmentDomainControl_TenantId,
            Controls_Reference,
            Controls_Name
        from {{ ref("vwAssessmentDomainControl") }} adc
        join {{ ref("vwControls") }} c on c.Controls_Id = adc.AssessmentDomainControl_ControlsId
    ),
    final as (
        select
            'Authority' as Requirement_Type,
            AssessmentDomainProvision_AssessmentDomainId AssessmentDomain_Id,
            AssessmentDomainProvision_Id Requirement_Id,
            AssessmentDomainProvision_TenantId Requirement_TenantId,
            AuthorityProvision_ReferenceId Requirement_ReferenceId,
            AuthorityProvision_Name Requirement_Name
        from auth_based
        union all
        select
            'Control' as Requirement_Type,
            AssessmentDomainControl_AssessmentDomainId AssessmentDomain_Id,
            AssessmentDomainControl_Id Requirement_Id,
            AssessmentDomainControl_TenantId Requirement_TenantId,
            Controls_Reference Requirement_ReferenceId,
            Controls_Name Requirement_Name
        from control_based
    )
select 
Requirement_Type,
AssessmentDomain_Id,
Requirement_Id,
Requirement_TenantId,
Requirement_ReferenceId,
Requirement_Name
from final

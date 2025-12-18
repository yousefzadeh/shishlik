with
    question_provision_authority as (
        select distinct
            'Provision' as relation,
            pq.ProvisionQuestion_QuestionId,
            ap.AuthorityProvision_Id Direct_AuthorityProvisionId,
            ap.AuthorityProvision_AuthorityId Direct_AuthorityId
        from {{ ref("vwProvisionQuestion") }} pq
        join
            {{ ref("vwAuthorityProvision") }} ap on pq.ProvisionQuestion_AuthorityProvisionId = ap.AuthorityProvision_Id
    ),
    question_control_provision_authority as (
        select
            'Control Provision' as relation,
            cq.ControlQuestion_QuestionId,
            pc.ProvisionControl_AuthorityReferenceId,
            ap.AuthorityProvision_AuthorityId
        from {{ ref("vwControlQuestion") }} cq
        join {{ ref("vwProvisionControl") }} pc on cq.ControlQuestion_ControlsId = pc.ProvisionControl_ControlsId
        join {{ ref("vwAuthorityProvision") }} ap on pc.ProvisionControl_AuthorityReferenceId = ap.AuthorityProvision_Id
    ),
    final as (
        select *
        from question_provision_authority
        union all
        select *
        from question_control_provision_authority
    )
select *
from final

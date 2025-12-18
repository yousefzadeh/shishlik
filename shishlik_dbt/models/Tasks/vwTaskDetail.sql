with
    uni as (
        -- Risk Treatment Plan
        select distinct
            'Risk Treatment Plan'[Type],
            rtp.RiskTreatmentPlan_Id ActionId,
            rtp.RiskTreatmentPlan_TenantId TenantId,
            rtp.RiskTreatmentPlan_TreatmentName Title,
            rtp.RiskTreatmentPlan_TreatmentDescription Description,
            rtp.RiskTreatmentPlan_TreatmentDate DueDate,
            rtp.RiskTreatmentPlan_TreatmentCompletedDate CompletedDate,
            -- 1 Task_IsCurrent,
            rtp.RiskTreatmentPlan_DueDateStatus DueDateStatus,
            rtp.RiskTreatmentPlan_StatusCode Status,
            rtpo.OwnerText AssigneeFilter,
            rtpa.OwnerList AssigneeList,
            rtpc.RiskTreatmentPlanComment_Comment Task_Comment
        from {{ ref("vwRisk") }} r
        left join {{ ref("vwRiskTreatmentPlanAssociation") }} rtpas
        on rtpas.RiskTreatmentPlanAssociation_RiskId = r.Risk_Id
        left join {{ ref("vwRiskTreatmentPlan") }} rtp
        on rtp.RiskTreatmentPlan_Id = rtpas.RiskTreatmentPlanAssociation_RiskTreatmentPlanId
        left join
            {{ ref("vwRiskTreatmentPlanOwnerFilter") }} rtpo
            on rtpo.RiskTreatmentPlanOwner_RiskTreatmentPlanId = rtp.RiskTreatmentPlan_Id
        left join
            {{ ref("vwRiskTreatmentPlanAttributeList") }} rtpa on rtpa.RiskTreatmentPlan_Id = rtp.RiskTreatmentPlan_Id
        left join {{ ref("vwRiskTreatmentPlanComment") }} rtpc
		on rtpc.RiskTreatmentPlanComment_RiskTreatmentPlanId = rtp.RiskTreatmentPlan_Id
        
        union all

        -- Issue Action
        select
            'Issue Task'[Type],
            ia.IssueAction_Id ActionId,
            ia.IssueAction_TenantId TenantId,
            ia.IssueAction_Title Title,
            ia.IssueAction_Description Description,
            ia.IssueAction_DueDate DueDate,
            ia.IssueAction_CompletedDate CompletedDate,
            -- 1 Task_IsCurrent,
            ia.IssueAction_DueDateStatus DueDateStatus,
            ia.IssueAction_StatusCode Status,
            au.AbpUsers_FullName AssigneeFilter,
            au.AbpUsers_FullName AssigneeList,
            iac.IssueActionComment_Comment Task_Comment
        from {{ ref("vwIssues") }} i
        left join {{ ref("vwIssueAction") }} ia
        on ia.IssueAction_IssueId = i.Issues_Id
        and ia.IssueAction_TenantId = i.Issues_TenantId
        left join
            {{ ref("vwAbpUser") }} au
            on au.AbpUsers_Id = ia.IssueAction_UserId
            and au.AbpUsers_TenantId = ia.IssueAction_TenantId
        left join {{ ref("vwIssueActionComment") }} iac
		on iac.IssueActionComment_IssueActionId = ia.IssueAction_Id
        where ia.IssueAction_Deprecated = 0
        
        union all

        -- Responsibility
        select distinct
            [Type],
            ActionId,
            TenantId,
            Title,
            Description,
            DueDate,
            CompletedDate,

            DueDateStatus,
            Status,
            AssigneeFilter,
            AssigneeList,
            rc.ResponsibilityComment_Comment Task_Comment

        from {{ ref("vwResponsibilityDetail") }} rd
        left join {{ ref("vwResponsibilityComment") }} rc
		on rc.ResponsibilityComment_ResponsibilityId = rd.Statement_Id


        union all

        -- Register Tasks
        select
            r.Register_RegisterName [Type],
            ia.IssueAction_Id ActionId,
            ia.IssueAction_TenantId TenantId,
            ia.IssueAction_Title Title,
            ia.IssueAction_Description Description,
            ia.IssueAction_DueDate DueDate,
            ia.IssueAction_CompletedDate CompletedDate,
            ia.IssueAction_DueDateStatus DueDateStatus,
            ia.IssueAction_StatusCode Status,
            au.AbpUsers_FullName AssigneeFilter,
            au.AbpUsers_FullName AssigneeList,
            iac.IssueActionComment_Comment Task_Comment
        from {{ ref("vwRegister") }} r
        join {{ ref("vwRegisterRecord") }} rr
        on rr.RegisterRecord_RegisterId = r.Register_Id
        left join {{ ref("vwIssueAction") }} ia
        on ia.IssueAction_IssueId = rr.RegisterRecord_Id
        and ia.IssueAction_TenantId = rr.RegisterRecord_TenantId
        left join {{ ref("vwAbpUser") }} au
        on au.AbpUsers_Id = ia.IssueAction_UserId
        and au.AbpUsers_TenantId = ia.IssueAction_TenantId
        left join {{ ref("vwIssueActionComment") }} iac
		on iac.IssueActionComment_IssueActionId = ia.IssueAction_Id
        where ia.IssueAction_Deprecated = 0
    )

select
    [Type],
    ActionId,
    TenantId,
    Title,
    Description,
    DueDate,
    CompletedDate,
    DueDateStatus,
    Status,
    AssigneeFilter,
    AssigneeList,
    Task_Comment
from
    uni
    -- where uni.TenantId = 1838
    -- New --> #18a0fb
    -- In-Progress --> #e95a38
    -- Completed -> #009a5e
    -- Overdue --> #e4001e
    

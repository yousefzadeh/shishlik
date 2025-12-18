with
    risk_review_assignment as (
        select
            r.Risk_TenantId Tenant_Id,
            r.Risk_Id RiskId,
            rr.RiskReview_Id RiskReviewId,
            rrt.UserId,
            rrt.RoleId,  -- Need to decode this - Owner, Collaborator
            rrt.Status,
            case rrt.Status when 0 then 'Not Responded' when 1 then 'Completed' else 'Unknown' end Status_Code
        from {{ ref("vwRisk") }} r
        join {{ ref("vwRiskReview") }} rr on rr.RiskReview_Id = r.Risk_RiskReviewId
        left join {{ source("risk_models", "RiskReviewTeam") }} rrt on rrt.RiskReviewId = rr.RiskReview_Id
        where rrt.IsDeleted = 0
    ),
    risk_review_completed as (
        select
            trr.TeamRiskRating_TenantId TenantId,
            trr.TeamRiskRating_RiskId RiskId,
            r.Risk_RiskReviewId RiskReviewId,
            u.AbpUsers_Id UserId,
            u.AbpUsers_EmailAddress EmailAddress,
            x_tpa.ThirdPartyAttributes_Label X_Label,
            y_tpa.ThirdPartyAttributes_Label Y_Label,
            rating_tpa.ThirdPartyAttributes_Label Rating,
            coalesce(trr.TeamRiskRating_LastModificationTime, trr.TeamRiskRating_CreationTime) CompletedDate
        from {{ ref("vwTeamRiskRating") }} trr
        join {{ ref("vwRisk") }} r on trr.TeamRiskRating_RiskId = r.Risk_Id
        left join {{ ref("vwAbpUser") }} u on trr.TeamRiskRating_UserId = u.AbpUsers_Id
        left join
            {{ ref("vwThirdPartyAttributes") }} x_tpa
            on trr.TeamRiskRating_XAxisThirdPartyAttributeId = x_tpa.ThirdPartyAttributes_Id
        left join
            {{ ref("vwThirdPartyAttributes") }} y_tpa
            on trr.TeamRiskRating_YAxisThirdPartyAttributeId = y_tpa.ThirdPartyAttributes_Id
        left join
            {{ ref("vwThirdPartyAttributes") }} rating_tpa
            on trr.TeamRiskRating_RiskRatingId = rating_tpa.ThirdPartyAttributes_Id
    ),
    risk_review_progress as (
        select rra.*, rrc.EmailAddress, rrc.X_Label, rrc.Y_Label, rrc.Rating, rrc.CompletedDate
        from risk_review_assignment rra
        left join
            risk_review_completed rrc
            on rra.RiskId = rrc.RiskId
            and rra.RiskReviewId = rrc.RiskReviewId
            and rra.UserId = rrc.UserId
    )
select
    rrp.*,
    case
        when (count(*) over (partition by RiskReviewId)) = (count(Rating) over (partition by RiskReviewId))
        then 'Completed'
        else 'Incomplete'
    end RiskReviewRating_Status
from risk_review_progress rrp

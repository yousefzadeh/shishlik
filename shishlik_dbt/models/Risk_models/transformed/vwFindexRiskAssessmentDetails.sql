with
    base as (
        select
            "RISKASSESSMENT"."RiskAssessment_RiskLabelIsCurrent",
            "RISKASSESSMENT"."RiskAssessment_TenantId",
            "RISKASSESSMENT"."RiskAssessment_RiskId"
        from {{ ref("vwRiskAssessment") }} as "RISKASSESSMENT"
    ),
    Inherent as (
        select
            "RISKASSESSMENT"."RiskAssessment_TenantId",
            "RISKASSESSMENT"."RiskAssessment_RiskId",
            case
                when "RISKASSESSMENT"."RiskAssessment_Label" = 'Inherent' then "RISKASSESSMENT"."RiskAssessment_Label"
            end as Inherent_Label,
            case
                when "RISKASSESSMENT"."RiskAssessment_Label" = 'Inherent'
                then "RISKASSESSMENTATTRIBUTELIST"."Likelihood"
            end as Inherent_Likelihood,
            case
                when "RISKASSESSMENT"."RiskAssessment_Label" = 'Inherent' then "RISKASSESSMENTATTRIBUTELIST"."Impact"
            end as Inherent_Impact,
            case
                when "RISKASSESSMENT"."RiskAssessment_Label" = 'Inherent'
                then "RISKASSESSMENTATTRIBUTELIST"."Rating_Label"
            end as Inherent_RiskRating,
            (
                case
                    when "RISKASSESSMENT"."RiskAssessment_Label" = 'Inherent'
                    then "RISKASSESSMENTATTRIBUTELIST"."Rating_Label"
                end
                + ' ['
                + REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    case
                                        when "RISKASSESSMENT"."RiskAssessment_Label" = 'Inherent'
                                        then "RISKASSESSMENTATTRIBUTELIST"."Likelihood"
                                    end,
                                    '1 - ',
                                    ''
                                ),
                                '2 - ',
                                ''
                            ),
                            '3 - ',
                            ''
                        ),
                        '4 - ',
                        ''
                    ),
                    '5 - ',
                    ''
                )
                + '/ '
                + REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    case
                                        when "RISKASSESSMENT"."RiskAssessment_Label" = 'Inherent'
                                        then "RISKASSESSMENTATTRIBUTELIST"."Impact"
                                    end,
                                    '1 - ',
                                    ''
                                ),
                                '2 - ',
                                ''
                            ),
                            '3 - ',
                            ''
                        ),
                        '4 - ',
                        ''
                    ),
                    '5 - ',
                    ''
                )
                + ']'
            ) Inherent_RR

        from {{ ref("vwRiskAssessment") }} as "RISKASSESSMENT" INNER hash
        join
            {{ ref("vwRiskAssessmentAttributeList") }} as "RISKASSESSMENTATTRIBUTELIST"
            on (
                "RISKASSESSMENT"."RiskAssessment_Id" = "RISKASSESSMENTATTRIBUTELIST"."RiskAssessmentId"
                and "RISKASSESSMENT"."RiskAssessment_TenantId" = "RISKASSESSMENTATTRIBUTELIST"."TenantId"
            )
        where
            case
                when "RISKASSESSMENT"."RiskAssessment_Label" = 'Inherent' then "RISKASSESSMENT"."RiskAssessment_Label"
            end
            is not null
            and "RISKASSESSMENT"."RiskAssessment_RiskLabelIsCurrent" = 1
    ),
    Currents as (
        select distinct
            "RISKASSESSMENT"."RiskAssessment_RiskLabelIsCurrent",
            "RISKASSESSMENT"."RiskAssessment_TenantId",
            "RISKASSESSMENT"."RiskAssessment_RiskId",
            case
                when "RISKASSESSMENT"."RiskAssessment_Label" = 'Current' then "RISKASSESSMENT"."RiskAssessment_Label"
            end as Current_Label,
            case
                when "RISKASSESSMENT"."RiskAssessment_Label" = 'Current' then "RISKASSESSMENTATTRIBUTELIST"."Likelihood"
            end as Current_Likelihood,
            case
                when "RISKASSESSMENT"."RiskAssessment_Label" = 'Current' then "RISKASSESSMENTATTRIBUTELIST"."Impact"
            end as Current_Impact,
            case
                when "RISKASSESSMENT"."RiskAssessment_Label" = 'Current'
                then "RISKASSESSMENTATTRIBUTELIST"."Rating_Label"
            end as Current_RiskRating,
            (
                case
                    when "RISKASSESSMENT"."RiskAssessment_Label" = 'Current'
                    then "RISKASSESSMENTATTRIBUTELIST"."Rating_Label"
                end
                + ' ['
                + REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    case
                                        when "RISKASSESSMENT"."RiskAssessment_Label" = 'Current'
                                        then "RISKASSESSMENTATTRIBUTELIST"."Likelihood"
                                    end,
                                    '1 - ',
                                    ''
                                ),
                                '2 - ',
                                ''
                            ),
                            '3 - ',
                            ''
                        ),
                        '4 - ',
                        ''
                    ),
                    '5 - ',
                    ''
                )
                + '/ '
                + REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    case
                                        when "RISKASSESSMENT"."RiskAssessment_Label" = 'Current'
                                        then "RISKASSESSMENTATTRIBUTELIST"."Impact"
                                    end,
                                    '1 - ',
                                    ''
                                ),
                                '2 - ',
                                ''
                            ),
                            '3 - ',
                            ''
                        ),
                        '4 - ',
                        ''
                    ),
                    '5 - ',
                    ''
                )
                + ']'
            ) Current_RR
        from {{ ref("vwRiskAssessment") }} as "RISKASSESSMENT" INNER hash
        join
            {{ ref("vwRiskAssessmentAttributeList") }} as "RISKASSESSMENTATTRIBUTELIST"
            on (
                "RISKASSESSMENT"."RiskAssessment_Id" = "RISKASSESSMENTATTRIBUTELIST"."RiskAssessmentId"
                and "RISKASSESSMENT"."RiskAssessment_TenantId" = "RISKASSESSMENTATTRIBUTELIST"."TenantId"
            )

        where
            case
                when "RISKASSESSMENT"."RiskAssessment_Label" = 'Current' then "RISKASSESSMENT"."RiskAssessment_Label"
            end
            is not null
            and "RISKASSESSMENT"."RiskAssessment_RiskLabelIsCurrent" = 1
    ),
    Target as (
        select
            "RISKASSESSMENT"."RiskAssessment_TenantId",
            "RISKASSESSMENT"."RiskAssessment_RiskId",
            case
                when "RISKASSESSMENT"."RiskAssessment_Label" = 'Target' then "RISKASSESSMENT"."RiskAssessment_Label"
            end as Target_Label,
            case
                when "RISKASSESSMENT"."RiskAssessment_Label" = 'Target' then "RISKASSESSMENTATTRIBUTELIST"."Likelihood"
            end as Target_Likelihood,
            case
                when "RISKASSESSMENT"."RiskAssessment_Label" = 'Target' then "RISKASSESSMENTATTRIBUTELIST"."Impact"
            end as Target_Impact,
            case
                when "RISKASSESSMENT"."RiskAssessment_Label" = 'Target'
                then "RISKASSESSMENTATTRIBUTELIST"."Rating_Label"
            end as Target_RiskRating,
            (
                case
                    when "RISKASSESSMENT"."RiskAssessment_Label" = 'Target'
                    then "RISKASSESSMENTATTRIBUTELIST"."Rating_Label"
                end
                + ' ['
                + REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    case
                                        when "RISKASSESSMENT"."RiskAssessment_Label" = 'Target'
                                        then "RISKASSESSMENTATTRIBUTELIST"."Likelihood"
                                    end,
                                    '1 - ',
                                    ''
                                ),
                                '2 - ',
                                ''
                            ),
                            '3 - ',
                            ''
                        ),
                        '4 - ',
                        ''
                    ),
                    '5 - ',
                    ''
                )
                + '/ '
                + REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    case
                                        when "RISKASSESSMENT"."RiskAssessment_Label" = 'Target'
                                        then "RISKASSESSMENTATTRIBUTELIST"."Impact"
                                    end,
                                    '1 - ',
                                    ''
                                ),
                                '2 - ',
                                ''
                            ),
                            '3 - ',
                            ''
                        ),
                        '4 - ',
                        ''
                    ),
                    '5 - ',
                    ''
                )
                + ']'
            ) Target_RR

        from {{ ref("vwRiskAssessment") }} as "RISKASSESSMENT" INNER hash
        join
            {{ ref("vwRiskAssessmentAttributeList") }} as "RISKASSESSMENTATTRIBUTELIST"
            on (
                "RISKASSESSMENT"."RiskAssessment_Id" = "RISKASSESSMENTATTRIBUTELIST"."RiskAssessmentId"
                and "RISKASSESSMENT"."RiskAssessment_TenantId" = "RISKASSESSMENTATTRIBUTELIST"."TenantId"
            )
        where
            case
                when "RISKASSESSMENT"."RiskAssessment_Label" = 'Target' then "RISKASSESSMENT"."RiskAssessment_Label"
            end
            is not null
            and "RISKASSESSMENT"."RiskAssessment_RiskLabelIsCurrent" = 1
    )

select distinct
    base.RiskAssessment_TenantId,
    base.RiskAssessment_RiskId,
    Inherent.Inherent_Label,
    Inherent.Inherent_Likelihood,
    Inherent.Inherent_Impact,
    Inherent.Inherent_RiskRating,
    Inherent.Inherent_RR,
    Currents.Current_Label,
    Currents.Current_Likelihood,
    Currents.Current_Impact,
    Currents.Current_RiskRating,
    Currents.Current_RR,
    Target.Target_Label,
    Target.Target_Likelihood,
    Target.Target_Impact,
    Target.Target_RiskRating,
    Target.Target_RR

from base left hash
join
    Inherent
    on base.RiskAssessment_TenantId = Inherent.RiskAssessment_TenantId
    and base.RiskAssessment_RiskId = Inherent.RiskAssessment_RiskId
    left hash
join
    Currents
    on base.RiskAssessment_TenantId = Currents.RiskAssessment_TenantId
    and base.RiskAssessment_RiskId = Currents.RiskAssessment_RiskId
    left hash
join
    Target
    on base.RiskAssessment_TenantId = Target.RiskAssessment_TenantId
    and base.RiskAssessment_RiskId = Target.RiskAssessment_RiskId

    -- where base.RiskAssessment_TenantId = 1384
    -- and base.RiskAssessment_RiskId = 4173
    -- and base.RiskAssessment_Label != 'No label'
    

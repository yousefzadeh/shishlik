select top 10000
    T0.C2 as A25,
    T0.C4 as A26,
    T0.C6 as A27,
    T0.C8 as A28,
    T0.C10 as A29,
    T0.C12 as A30,
    T0.C14 as A31,
    T0.C16 as A32,
    T0.C18 as A33,
    T0.C20 as A34,
    T0.C22 as A35,
    T0.C24 as A36,
    T1.C44 as A45
from
    (
        select distinct
            "VWRISKMATRIXCONFIG"."y_attribute_id" as C2,
            "VWRISKMATRIXCONFIG"."ThirdPartyDynamicFieldConfiguration_TenantId" as C4,
            "VWRISKMATRIXCONFIG"."ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlName" as C6,
            "VWRISKMATRIXCONFIG"."ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlName" as C8,
            "VWRISKMATRIXCONFIG"."ThirdPartyDynamicFieldData_YAxisAttributeLabel" as C10,
            "VWRISKMATRIXCONFIG"."YLabel_Order" as C12,
            "VWRISKMATRIXCONFIG"."x_attribute_id" as C14,
            "VWRISKMATRIXCONFIG"."ThirdPartyDynamicFieldData_XAxisAttributeLabel" as C16,
            "VWRISKMATRIXCONFIG"."XLabel_Order" as C18,
            "VWRISKMATRIXCONFIG"."ThirdPartyDynamicFieldData_DynamicColor" as C20,
            "VWRISKMATRIXCONFIG"."ThirdPartyDynamicFieldData_DynamicScoreValue" as C22,
            "VWRISKMATRIXCONFIG"."ThirdPartyDynamicFieldData_DynamicValue" as C24
        from {{ ref("vwRiskMatrixConfig") }} as "VWRISKMATRIXCONFIG"
        where
            ("VWRISKMATRIXCONFIG"."ThirdPartyDynamicFieldConfiguration_TenantId" in (1384))
            and ("VWRISKMATRIXCONFIG"."MatrixName" = 'Risk Dev')

    ) T0
left outer join
    (
        select distinct
            "RISK"."Risk_TenantId" as C38,
            "VWRISKASSESSMENTMATRIX"."y_attribute_Id" as C40,
            "VWRISKASSESSMENTMATRIX"."x_attribute_Id" as C42,
            COUNT(
                DISTINCT(
                    case
                        when "VWRISKASSESSMENTMATRIX"."RiskAssessment_RiskLabelIsCurrent" = 1
                        then "VWRISKASSESSMENTMATRIX"."RiskAssessmentId"
                    end
                )
            ) as C44
        from {{ ref("vwRisk") }} as "RISK"
        inner join
            {{ ref("vwRiskAssessmentMatrix") }} as "VWRISKASSESSMENTMATRIX"
            on ("RISK"."Risk_Id" = "VWRISKASSESSMENTMATRIX"."RiskId")
        where
            ("RISK"."Risk_TenantId" in (1384))
            and (
                ("RISK"."Risk_Status" = 1)
                and "VWRISKASSESSMENTMATRIX"."x_attribute_Id" is not NULL
                and "VWRISKASSESSMENTMATRIX"."y_attribute_Id" is not NULL
            )
        group by
            "VWRISKASSESSMENTMATRIX"."y_attribute_Id", "VWRISKASSESSMENTMATRIX"."x_attribute_Id", "RISK"."Risk_TenantId"

    ) T1
    on T0.C4 = T1.C38
    and T0.C14 = T1.C42
    and T0.C2 = T1.C40

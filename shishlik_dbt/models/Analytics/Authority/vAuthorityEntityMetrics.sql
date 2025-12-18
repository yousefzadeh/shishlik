select
a.TenantId,
a.Authority_Id,
a.Authority_Name,
Authority_Type,
Authority_Jurisdiction,
apcf.AuthorityProvision_CustomField,
ISNULL(assess.Num_of_Assessments, 0) Num_of_Assessments,
ISNULL(cset.Num_of_CtrlSets, 0) AS Num_of_CtrlSets,
ISNULL(ctrl.Num_of_Ctrls, 0) AS Num_of_Ctrls,
ISNULL(ris.Num_of_Risks, 0) AS Num_of_Risks,
ISNULL(iss.Num_of_Issues, 0) AS Num_of_Issues
FROM {{ref("vAuthority")}} a
JOIN {{ref("vAuthorityProvision")}} ap
ON a.Authority_Id = ap.Authority_Id
JOIN {{ref("vAuthorityProvisionCustomFields")}} apcf
ON ap.AuthorityProvision_Id = apcf.AuthorityProvision_Id
LEFT JOIN (
SELECT
TenantId,
Assessment_AuthorityId AS Authority_Id,
COUNT(DISTINCT Assessment_Id) AS Num_of_Assessments
FROM {{ref("vAssessment")}}
GROUP BY TenantId, Assessment_AuthorityId
) AS assess
ON a.TenantId = assess.TenantId
AND a.Authority_Id = assess.Authority_Id
LEFT JOIN (
SELECT
TenantId,
Authority_Id,
COUNT(DISTINCT ControlSet_Id) AS Num_of_CtrlSets
FROM {{ref("vAuthorityControlSet")}}
GROUP BY TenantId, Authority_Id
) AS cset
ON a.TenantId = cset.TenantId
AND a.Authority_Id = cset.Authority_Id
LEFT JOIN (
SELECT
    ap.Authority_Id,
    COUNT(DISTINCT apc.Controls_Id) AS Num_of_Ctrls
FROM {{ref("vAuthorityProvisionControl")}} apc
INNER JOIN {{ref("vAuthorityProvision")}} ap
    ON apc.AuthorityProvision_Id = ap.AuthorityProvision_Id
GROUP BY ap.Authority_Id
) AS ctrl
on a.Authority_Id = ctrl.Authority_Id
LEFT JOIN (
SELECT
    ap.Authority_Id,
    COUNT(DISTINCT apr.Risk_Id) AS Num_of_Risks
FROM {{ref("vAuthorityProvisionRisk")}} apr
INNER JOIN {{ref("vAuthorityProvision")}} ap
    ON apr.AuthorityProvision_Id = ap.AuthorityProvision_Id
   AND apr.Authority_Id = ap.Authority_Id
GROUP BY ap.Authority_Id
) AS ris
on a.Authority_Id = ris.Authority_Id
LEFT JOIN (
SELECT
    ap.Authority_Id,
    COUNT(DISTINCT api.Issue_Id) AS Num_of_Issues
FROM {{ref("vAuthorityProvisionIssue")}} api
INNER JOIN {{ref("vAuthorityProvision")}} ap
    ON api.Authority_Id = ap.Authority_Id
   and api.AuthorityProvision_Id = ap.AuthorityProvision_Id
GROUP BY ap.Authority_Id
) AS iss
on a.Authority_Id = iss.Authority_Id
WHERE apcf.AuthorityProvision_CustomField IS NOT NULL
GROUP BY
a.TenantId,
a.Authority_Id,
a.Authority_Name,
Authority_Type,
Authority_Jurisdiction,
apcf.AuthorityProvision_CustomField,
ISNULL(assess.Num_of_Assessments, 0),
ISNULL(cset.Num_of_CtrlSets, 0),
ISNULL(ctrl.Num_of_Ctrls, 0),
ISNULL(ris.Num_of_Risks, 0),
ISNULL(iss.Num_of_Issues, 0)
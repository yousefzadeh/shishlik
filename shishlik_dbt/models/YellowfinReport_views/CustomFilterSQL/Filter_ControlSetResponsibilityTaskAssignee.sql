with final as (
   SELECT 
      cs.Policy_TenantId,
      cs.Policy_Name,
      c.Controls_Name,
      resp.Responsibility_Title,
      task.Assignee
   FROM {{ ref("vwResponsibility") }} AS resp
   INNER JOIN {{ ref("vwResponsibilityControl") }} AS rc
   ON (
      resp.Responsibility_Id = rc.ResponsibilityControl_ResponsibilityId
   and resp.Responsibility_TenantId = rc.ResponsibilityControl_TenantId
   )
   INNER JOIN {{ ref("vwControls") }} AS c
   ON (
      rc.ResponsibilityControl_ControlId = c.Controls_Id
   and rc.ResponsibilityControl_TenantId = c.Controls_TenantId
   )
   INNER JOIN {{ ref("vwPolicyDomain") }} AS pd
   ON (
      c.Controls_PolicyDomainId = pd.PolicyDomain_Id
   and c.Controls_TenantId = pd.PolicyDomain_TenantId
   )
   INNER JOIN {{ ref("vwPolicy") }} AS cs
   ON (
      pd.PolicyDomain_PolicyId = cs.Policy_Id
   and pd.PolicyDomain_TenantId = cs.Policy_TenantId
   )
   LEFT OUTER JOIN {{ ref("vwResponsibilityTaskDetail") }} AS task
   ON (
      resp.Responsibility_Id = task.StatementResponse_StatementId
   and resp.Responsibility_TenantId = task.StatementResponse_TenantId
   )
   WHERE cs.Policy_StatusCode = 'Published'
)
SELECT 
   Policy_TenantId,
   Policy_Name,
   Controls_Name,
   Responsibility_Title,
   Assignee
FROM final
{# where Policy_TenantId IN (1384) #}

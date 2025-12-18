{{ config(materialized="view") }}

with
    tp as (
        select IssueThirdParty_IssueId, IssueThirdParty_TenantId, IssueThirdParty_TenantVendorId
        from {{ ref("vwIssueThirdParty") }}
    ),
    tv as (select TenantVendor_Id, TenantVendor_Name from {{ ref("vwTenantVendor") }}),
    base as (
        select
            tp.IssueThirdParty_IssueId,
            tp.IssueThirdParty_TenantId,
            STRING_AGG(tv.TenantVendor_Name, ',') as ThirdPartyList
        from tp
        inner join tv on tp.IssueThirdParty_TenantVendorId = tv.TenantVendor_Id
        group by tp.IssueThirdParty_IssueId, tp.IssueThirdParty_TenantId
    )

select [IssueThirdParty_IssueId], [IssueThirdParty_TenantId],{{ col_rename("ThirdPartyList", "TenantVendor") }}
from
    base

    /*
'SELECT tp.IssueThirdParty_IssueId,tp.IssueThirdParty_TenantId,STRING_AGG(tv.Name,',') as ThirdPartyList
FROM [6clicks-dev-ihsopk].test_dbt_cicd.vwIssueThirdParty tp 
-- Note that the line below is using the dbo schema. You might want to change this.
INNER JOIN [6clicks-dev-ihsopk].dbo.TenantVendor tv
    ON tp.IssueThirdParty_TenantVendorId = tv.Id
GROUP BY tp.IssueThirdParty_IssueId,tp.IssueThirdParty_TenantId'
*/
    

{{ config(materialized="view") }}
WITH Asset as (
    select iss.id as Asset_id
        ,cast(iss.[Name] as nvarchar(2000)) AssetName
        ,cast(iss.[Description] as nvarchar(4000)) Asset_Description
        ,iss.[TenantId]
        ,at2.AbpTenants_Name
        ,Coalesce(iss.RecordedDate,iss.CreationTime) ReportedTime
        ,Coalesce(iss.LastModificationTime,iss.CreationTime) UpdatedTime
    from {{ source("issue_models", "Issues") }}  iss
            left join{{ source("issue_models", "EntityRegister") }}  er on er.id = iss.EntityRegisterId
            left join {{ ref("vwAbpTenants") }} at2 on at2.AbpTenants_Id = iss.TenantId
    WHERE iss.IsDeleted = 0 and er.IsDeleted = 0
            and iss.IsArchived = 0
            and iss.Status != 100
            and er.EntityType = 5
)

, AssetToAssessment AS (
    SELECT distinct
        a.Asset_id, a.AssetName, a.Asset_Description, a.TenantId, a.AbpTenants_Name
        ,cast(asst.[Name] as nvarchar(2000)) as AssessmentName
        ,asst.Status as AssessmentStatusCode
        ,min(asst.ResponseStartedDate ) over(partition by a.Asset_id,a.AssetName, cast(asst.[Name] as nvarchar(2000)), cast(auth.[Name] as nvarchar(2000))) as Assessment_ResponseStartdate 
        ,max(asst.ResponseCompletedDate) over(partition by a.Asset_id,a.AssetName, cast(asst.[Name] as nvarchar(2000)), cast(auth.[Name] as nvarchar(2000))) as Assessment_LatestResponseCompletedDate 
        --,DATEDIFF( day, ResponseStartedDate,coalesce(ResponseCompletedDate, current_date)) as ResponseCompletionDays
        ,cast(auth.[Name] as nvarchar(2000)) as AuthorityName
        ,cast(auth.[Description] as nvarchar(4000)) as AuthorityDescription
        ,cast(auth.[Body] as nvarchar(500)) as AuthorityBody
        ,cast(auth.[AuthoritySector] as nvarchar(100)) as AuthoritySector
    FROM Asset a 
            join {{ source("assessment_models", "AssessmentScopeRegisterItem") }} ar
            on a.Asset_id = ar.RegisterItemId
            left join {{ source("assessment_models", "Assessment") }} asst
            on ar.AssessmentId = asst.id
            left join {{ source("assessment_models", "Authority") }} auth
            on asst.AuthorityId = auth.Id
    WHERE  asst.IsDeleted = 0
        --and asst.ResponseStartedDate is not null
)
, main as (
    select distinct 
        Asset_id
        ,AssetName
        ,Asset_Description
        ,TenantId
        ,AbpTenants_Name
        ,AssessmentName
        ,AssessmentStatusCode
        ,Assessment_ResponseStartdate
        ,Assessment_LatestResponseCompletedDate
        ,AuthorityName
        ,AuthorityDescription
        ,AuthorityBody
        ,AuthoritySector
        ,case
            when AssessmentStatusCode = 1 then 'Draft'
            when AssessmentStatusCode= 2 then 'Approved'
            when AssessmentStatusCode= 3 then 'Published'
            when AssessmentStatusCode= 4 then 'Completed'
            when AssessmentStatusCode= 5 then 'Closed'
            when AssessmentStatusCode= 6 then 'Reviewed'
            when AssessmentStatusCode= 7 then 'In Progress'
            when AssessmentStatusCode= 8 then 'Cancelled'
            else 'Undefined' end AssessmentStatus
        , CASE
            WHEN Assessment_ResponseStartdate IS NOT NULL
              THEN DATEDIFF(day, Assessment_ResponseStartdate, COALESCE(Assessment_LatestResponseCompletedDate, GETDATE()))
            WHEN Assessment_LatestResponseCompletedDate IS NOT NULL
              THEN NULL  -- or 0, if that’s the desired business rule
            ELSE NULL
          END AS ResponseCompletionDays
        , RANK() OVER (
            PARTITION BY Asset_id, AbpTenants_Name,AssessmentName, AssessmentStatusCode
            ORDER BY COALESCE(Assessment_ResponseStartdate, CAST('2000-01-01 00:00:00' AS datetime)) DESC
          ) AS rnk
    from AssetToAssessment
)
SELECT Asset_id
        ,AssetName
        ,Asset_Description
        ,TenantId
        ,AbpTenants_Name
        ,AssessmentName
        ,AssessmentStatusCode
        ,Assessment_ResponseStartdate
        ,Assessment_LatestResponseCompletedDate
        ,AuthorityName
        ,AuthorityDescription
        ,AuthorityBody
        ,AuthoritySector  
        ,AssessmentStatus
        ,ResponseCompletionDays
from main
where rnk = 1;
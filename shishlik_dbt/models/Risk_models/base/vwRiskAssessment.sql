{{ config(materialized="view") }}
{# 
DOC START
  - name: vwRiskAssessment
    description: |
        List of all RiskAssessments for a given RiskId
        - Included deleted RiskAssessments
        - included a row with zero RiskAssessmentId where no assessments are done for a risk
    columns:
      - name: RiskAssessment_Id
        description: Id of the RiskAssessment
      - name: RiskAssessment_CreationTime
        description: CreationTime of the RiskAssessment
      - name: RiskAssessment_CreatorUserId
        description: CreatorUserId of the RiskAssessment
      - name: RiskAssessment_LastModificationTime
        description: LastModificationTime of the RiskAssessment
      - name: RiskAssessment_LastModifierUserId
        description: LastModifierUserId of the RiskAssessment
      - name: RiskAssessment_IsDeleted
        description: IsDeleted flag (1/0) of the RiskAssessment
      - name: RiskAssessment_DeleterUserId
        description: DeleterUserId of the RiskAssessment
      - name: RiskAssessment_DeletionTime
        description: DeletionTime of the RiskAssessment
      - name: RiskAssessment_TenantId
        description: TenantId of the RiskAssessment for data access
      - name: RiskAssessment_RiskId
        description: RiskId of the RiskAssessment
      - name: RiskAssessment_FavouriteId
        description: |
            Favorite RiskAssessment for a given RiskId
            Only 1 Favorite RiskAssessment for a given RiskId
            Can be NULL where no favorite assessment is selected
      - name: RiskAssessment_Label
        description: Label of the RiskAssessment
      - name: RiskAssessment_RiskLabelSequence
        description: Sequence of RiskAssessments for a given RiskAssessment_Label (1,2,3...)
      - name: RiskAssessment_RiskLabelIsCurrent
        description: Latest RiskAssessment for a given RiskAssessment_Label is tagged = 1
      - name: RiskAssessment_Title
        description: Displayed name of the RiskAssessment
      - name: RiskAssessment_SystemRiskAssessmentCode
        description: SystemRiskAssessmentCode of the RiskAssessment
      - name: RiskAssessment_AssessmentDate
        description: AssessmentDate of the RiskAssessment
      - name: RiskAssessment_LatestFlag
        description: |
            One risk can only have one favorite RiskAssessment
            Show the favorite RiskAssessment for a given RiskId
            if there is no favorite RiskAssessment, then the latest RiskAssessment is the favorite
DOC END
#}
with
    base as (
        -- Get ALL (including deleted) RiskAssessments for a given RiskId 
        select
            {{ system_fields_macro() }},
            [TenantId],
            [RiskId],
            [Title],
            [SystemRiskAssessmentCode],
            [AssessmentDate],
            -- Sorted by IsDeleted asc, CreationTime desc to pick the latest active RiskAssessment 
            case
                when row_number() over (partition by TenantId, RiskId order by IsDeleted asc, CreationTime desc) = 1
                then 1
                else 0
            end LatestFlag
        from {{ source("risk_models", "RiskAssessment") }}
    ),
    zero as (
        -- Add zero key rows where no assessments are done for a risk
        select
            0 Id,
            r.CreationTime,
            r.CreatorUserId,
            r.LastModificationTime,
            r.LastModifierUserId,
            r.IsDeleted,
            r.DeleterUserId,
            r.DeletionTime,
            r.TenantId,
            r.Id RiskId,
            'No Risk Assessment' Title,
            NULL SystemRiskAssessmentCode,
            NULL AssessmentDate,
            0 LatestFlag
        from {{ source("risk_models", "Risk") }} r
        left join base on r.Id = base.RiskId
        where base.RiskId is NULL
    ),
    base_zero as (
        -- for every riskid, a zero key is added where there are no assessments done
        select *
        from base
        union all
        select *
        from zero
    ),
    ra as (
        -- All Risk Assessments including deleted Assessments
        -- All Risk where No RiskAssessments are done - RiskAssesment_ID = 0
        -- Where Risks are deleted, RiskAssessments are also deleted
        select
            {{ col_rename("Id", "RiskAssessment") }},
            {{ col_rename("CreationTime", "RiskAssessment") }},
            {{ col_rename("CreatorUserId", "RiskAssessment") }},
            {{ col_rename("LastModificationTime", "RiskAssessment") }},
            {{ col_rename("LastModifierUserId", "RiskAssessment") }},
            {{ col_rename("IsDeleted", "RiskAssessment") }},
            {{ col_rename("DeleterUserId", "RiskAssessment") }},
            {{ col_rename("DeletionTime", "RiskAssessment") }},
            {{ col_rename("TenantId", "RiskAssessment") }},
            {{ col_rename("RiskId", "RiskAssessment") }},
            {{ col_rename("Title", "RiskAssessment") }},
            {{ col_rename("SystemRiskAssessmentCode", "RiskAssessment") }},
            {{ col_rename("AssessmentDate", "RiskAssessment") }},
            {{ col_rename("LatestFlag", "RiskAssessment") }}
        from base_zero
    ),
    label as (
        select *
        from {{ ref("vwRiskAssessmentCustomAttributeData") }} racad
        join
            {{ ref("vwThirdPartyAttributes") }} tpa
            on racad.RiskAssessmentCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
        inner hash join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
            and tpc.ThirdPartyControl_EntityType = 4
            and tpc.ThirdPartyControl_Name = 'Risk Assessment Labels'

    ),
    ra_label as (
        -- RiskAssessment_Label is the label of the RiskAssessment with the following logic:
        -- 1. If there is no label, then 'No label'
        -- 2. If there is a label, then the label column in ThirdPartyAttributes table
        -- 3. If there is no RiskAssessment for a given RiskId, then 'No Risk Assessment'
        select
            ra.*,
            case
                -- RiskAssessment with no label
                when ral.RiskAssessmentCustomAttributeData_RiskAssessmentId is null
                then 'No label'
                -- No RiskAssessment for a that RiskId
                when ra.RiskAssessment_Id = 0
                    -- 'No Risk Assessment' Title defined in zero CTE
                then ra.RiskAssessment_Title  
                -- RiskAssessment with label
                else ral.ThirdPartyAttributes_Label
            end as RiskAssessment_Label
        from ra
        left join
            label ral
            on ra.RiskAssessment_Id = ral.RiskAssessmentCustomAttributeData_RiskAssessmentId
            and ra.RiskAssessment_TenantId = ral.RiskAssessmentCustomAttributeData_TenantId
    ),
    ra_seq as (
        select
            -- Sequence of RiskAssessments for a given RiskAssessment_Label by creation time -- needed in report column
            ROW_NUMBER() over (
                partition by RiskAssessment_RiskId, RiskAssessment_Label order by RiskAssessment_CreationTime desc
            ) RiskAssessment_RecentSeqId,
            -- Sequence of RiskAssessments for a given RiskAssessment_Label by descending creation time to get the LatestFlag
            ROW_NUMBER() over (
                partition by RiskAssessment_RiskId, RiskAssessment_Label order by RiskAssessment_CreationTime
            ) RiskAssessment_Sequence,
            RiskAssessment_Id,
            RiskAssessment_CreationTime,
            RiskAssessment_CreatorUserId,
            RiskAssessment_LastModificationTime,
            RiskAssessment_LastModifierUserId,
            RiskAssessment_IsDeleted,
            RiskAssessment_DeleterUserId,
            RiskAssessment_DeletionTime,
            RiskAssessment_TenantId,
            RiskAssessment_RiskId,
            RiskAssessment_Label,
            RiskAssessment_Title,
            RiskAssessment_SystemRiskAssessmentCode,
            RiskAssessment_AssessmentDate,
            RiskAssessment_LatestFlag
        from ra_label
        where RiskAssessment_IsDeleted = 0
    )
, final as (
    {# join to Risk to get FavouriteRiskAssessmentId #}
    select
        RiskAssessment_Id,
        RiskAssessment_CreationTime,
        RiskAssessment_CreatorUserId,
        RiskAssessment_LastModificationTime,
        RiskAssessment_LastModifierUserId,
        RiskAssessment_IsDeleted,
        RiskAssessment_DeleterUserId,
        RiskAssessment_DeletionTime,
        RiskAssessment_TenantId,
        RiskAssessment_RiskId,
        {# 
            Favorite RiskAssessment for a given RiskId
            Only 1 Favorite RiskAssessment for a given RiskId
            Can be NULL where no favorite ID is selected
        #}
        r.Risk_FavouriteRiskAssessmentId RiskAssessment_FavouriteId,
        cast(RiskAssessment_Label as nvarchar(4000)) RiskAssessment_Label,
        -- Sequence of RiskAssessments for a given RiskAssessment_Label
        RiskAssessment_Sequence RiskAssessment_RiskLabelSequence, -- misleading column name should be SequenceByRiskLabel
        -- Latest RiskAssessment for a given RiskAssessment_Label
        case when RiskAssessment_RecentSeqId = 1 then 1 else 0 end RiskAssessment_RiskLabelIsCurrent, -- misleading column name
        cast(RiskAssessment_Title as nvarchar(4000)) RiskAssessment_Title,
        RiskAssessment_SystemRiskAssessmentCode,
        RiskAssessment_AssessmentDate,
        {#     
            One risk can only have one favorite RiskAssessment
            Show the favorite RiskAssessment for a given RiskId
            if there is no favorite RiskAssessment, then the latest RiskAssessment is the favorite
        #}
        case
            when r.Risk_FavouriteRiskAssessmentId is not null -- this assessment is the favorite
            then 1
            else RiskAssessment_LatestFlag -- 0 if not latest, 1 if latest
        end RiskAssessment_LatestFlag -- misleading column name should be called FavouriteFlag
    from ra_seq
    left join {{ ref("vwRisk") }} r on r.Risk_FavouriteRiskAssessmentId = ra_seq.RiskAssessment_Id
)
select * from final
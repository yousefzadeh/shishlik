-- Risk Treatment Plan Log Details to fetch Change Time of Current Risk Treatment Plan
with
    uni as (
        select distinct
            c.Id as [ChangeId],
            cast(c.EntityId as int) EntityId,
            c.EntityTypeFullName,
            cs.Id as [SetId],
            cs.ExtensionData,
            -- , case when pc.NewValue =1 then c.ChangeTime end CompletedTime
            case when pc.NewValue = 100 then c.ChangeTime end CompletedTime,
            cs.CreationTime,
            c.ChangeType,
            cs.UserId,
            cs.TenantId,
            cs.Reason,
            pc.Id as [PropChangeId],
            pc.PropertyName,
            pc.OriginalValue,
            pc.NewValue,
            case
                when lead(c.ChangeTime) over (partition by cs.TenantId, c.EntityId order by pc.Id) is null then 1 else 0
            end IsCurrent,
            -- ,case when lead(c.ChangeTime) over (partition by cs.TenantId, c.EntityId order by pc.Id) is null then 1
            -- else 0 end CompletedDate
            row_number() over (partition by cs.TenantId, c.EntityId order by pc.Id desc) as CompletedDate
        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        where PropertyName = 'Stage' and EntityTypeFullName = 'LegalRegTech.Issues.Issue'
    -- and cs.TenantId = 1838
    -- c.EntityTypeFullName <> 'LegalRegTech.Risk.Risk'
    -- and c.EntityId in (1882, 2392) --RiskId
    )

select
    [ChangeId],
    EntityId,
    EntityTypeFullName,
    [SetId],
    ExtensionData,
    CompletedTime,
    ChangeType,
    UserId,
    TenantId,
    Reason,
    [PropChangeId],
    PropertyName,
    OriginalValue,
    NewValue

from uni
where IsCurrent = 1

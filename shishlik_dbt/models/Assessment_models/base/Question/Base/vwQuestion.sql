{{ config(materialized="view") }}
with
    base as (
        select
            /* [Id]
      ,[CreationTime]
      ,[CreatorUserId]
      ,[LastModificationTime]
      ,[LastModifierUserId]
      ,[IsDeleted]
      ,[DeleterUserId]
      ,[DeletionTime] */
            {{ system_fields_macro() }},
            cast([Name] as nvarchar(500))[Name],
            cast([Description] as nvarchar(4000)) Description,
            [Order],
            [File],
            [Type],
            [AssessmentDomainId],
            [ComponentStr] ComponentStr,
            [VendorDocumentRequired],
            cast([Code] as nvarchar(100))[Code],
            [TenantId],
            [Weighting],
            [RiskStatus],
            [Condition],
            [HasConditionalLogic],
            [IsVisibleIfConditional],
            [HiddenInSurveyForConditional],
            [DisplayDocumentUpload],
            [QuestionGroupId],
            cast([Suborder] as nvarchar(4000)) Suborder,
            [RootQuestionId],
            [ParentQuestionId],
            [IsMandatory],
            cast([IdRef] as nvarchar(100))[IdRef],
            cast(concat('D', [AssessmentDomainId], '_', [IdRef], '#', [Code]) as nvarchar(4000)) as PK,  -- Business Key
            cast(coalesce(LastModificationTime, CreationTime) as smalldatetime) as UpdateTime
        -- TenantId seems to be covered by AssessmentDomainID
        from {{ source("assessment_models", "Question") }} {{ system_remove_IsDeleted() }}
    )

select distinct
    {{ col_rename("ID", "Question") }},
    {{ col_rename("Name", "Question") }},
    {{ col_rename("Description", "Question") }},
    {{ col_rename("Order", "Question") }},

    {{ col_rename("File", "Question") }},
    {{ col_rename("Type", "Question") }},
    case
        when [Type] = 1
        then 'Yes No'
        when [Type] in (2, 5, 6, 10)
        then 'Choose One'
        when [Type] in (3, 7, 8)
        then 'Choose Many'
        when [Type] in (4, 9)
        then 'Free Text Response'
        else 'Undefined'
    end as [Question_TypeCode],
    {{ col_rename("AssessmentDomainId", "Question") }},
    {{ col_rename("ComponentStr", "Question") }},

    {{ col_rename("VendorDocumentRequired", "Question") }},
    {{ col_rename("Code", "Question") }},
    {{ col_rename("TenantId", "Question") }},
    {{ col_rename("Weighting", "Question") }},

    {{ col_rename("RiskStatus", "Question") }},
    {{ col_rename("Condition", "Question") }},
    {{ col_rename("HasConditionalLogic", "Question") }},
    {{ col_rename("IsVisibleIfConditional", "Question") }},

    {{ col_rename("HiddenInSurveyForConditional", "Question") }},
    {{ col_rename("DisplayDocumentUpload", "Question") }},
    {{ col_rename("QuestionGroupId", "Question") }},
    {{ col_rename("Suborder", "Question") }},

    {{ col_rename("RootQuestionId", "Question") }},
    {{ col_rename("ParentQuestionId", "Question") }},
    {{ col_rename("IsMandatory", "Question") }},
    {{ col_rename("IdRef", "Question") }},

    {{ col_rename("PK", "Question") }},
    {{ col_rename("UpdateTime", "Question") }}
from base

select
    a.ProvisionQuestion_QuestionId,
    '(' + a.Mapped_AuthorityName + ') ' + b.AuthorityCustom_FieldName as Mapped_CustomField_Filter
from {{ ref("vwMappedQuestionProvisionAuthority_source") }} as a
join
    {{ ref("vwProvisionCustomFieldValue_lambda") }} as b
    on a."Mapped_AuthorityId" = b."AuthorityCustom_AuthorityId"
    and a."Mapped_AuthorityProvisionId" = b."Provision_Id"

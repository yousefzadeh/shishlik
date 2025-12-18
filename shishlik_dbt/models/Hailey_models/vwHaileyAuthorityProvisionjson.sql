with cus_field as(
select 
    ap.AuthorityId,
    ap.Id AuthorityProvisionId,
    a.Name as AuthorityName,
    ap.ReferenceId, 
    ap.Name,
    ap.CreationTime CreationDate,
    js.CustomFieldId,
    js.CustomFieldName,
    js.CustomFieldValue
from {{ source("hailey_models", "AuthorityProvision") }} ap
join {{ source("hailey_models", "Authority") }} a
on a.Id = ap.AuthorityId
outer apply openjson(ap.CustomDataJson) WITH (  
                    CustomFieldId Int '$.Id',
                    CustomFieldName Varchar(200) '$.Name',
                    CustomFieldValue    nvarchar(4000) '$.Value'
                    ) as js
where ap.IsDeleted = 0
  and ISJSON(ap.CustomDataJson) = 1
)

select
    ap.AuthorityId,
    ap.AuthorityProvisionId,
    ap.AuthorityName,
    ap.ReferenceId,
    ap.Name,
    ap.CreationDate,
    ap.CustomFieldId,
    ap.CustomFieldName,
    ap.CustomFieldValue
from cus_field ap
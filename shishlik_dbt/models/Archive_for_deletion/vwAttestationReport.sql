{{ config(materialized="view") }}

with
    attes as (
        select
            ai.AttestationItems_ItemId,
            ai.AttestationItems_DisplayName,
            ai.AttestationItems_ItemEntityType,
            ai.AttestationItems_Version,
            a.Attestations_Id,
            a.Attestations_Name,
            a.Attestations_Description,
            a.Attestations_DueDate,
            a.Attestations_Status,
            a.Attestations_IsArchived,
            ai.AttestationItems_VersionDate,
            aa2.AttestationAttestors_UserId,
            aa2.AttestationAttestors_CompletionDate Attestation_Date,
            aa.AttestorApprovals_Status Response,
            aa.AttestorApprovals_Comment

        from {{ ref("vwAttestations") }} a
        join {{ ref("vwAttestationItems") }} ai on a.Attestations_Id = ai.AttestationItems_AttestationId
        join {{ ref("vwAttestorApprovals") }} aa on ai.AttestationItems_Id = aa.AttestorApprovals_AttestationItemId
        join {{ ref("vwAttestationAttestors") }} aa2 on a.Attestations_Id = aa2.AttestationAttestors_AttestationId

        where a.Attestations_IsArchived = 0
    )

select *
from attes

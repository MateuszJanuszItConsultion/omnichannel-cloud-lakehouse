# ADR-0013: Terraform state in Azure Storage

**Status:** Accepted
**Date:** 2026-10-11

## Context
Terraform needs remote state: CI must be able to run `terraform plan`, and a
local state file can be lost. The author already has an Azure subscription and
an Azure Storage account holding the Terraform state of project 1
(`airline-cloud-warehouse`, frozen).

## Decision
- State is stored in the existing storage account `terraformstatestoragemj`,
  shared with project 1, under the key `omnichannel.tfstate`.
  Container: `<TODO: tfstate (shared) or a dedicated container>`.
- This is a deliberate choice: not multiplying Azure objects for a portfolio
  project outweighs the coupling it introduces.
- Backend configuration is partial: `backend.tf` declares only `azurerm`;
  values live in `backend.hcl` (not committed), with `backend.hcl.example`
  in the repository.
- Access through Entra ID (`use_azuread_auth = true`), no storage account keys.
- On PowerShell, the init argument must be quoted:
  `terraform init "-backend-config=backend.hcl"`.

## Consequences
- Risks of the shared account, accepted knowingly:
  - a typo in `key` could overwrite the state of project 1;
  - cleaning up project 1 resources could delete this state as well.
- Mitigation: `<TODO: dedicated container, if chosen>`; the key is set only in
  `backend.hcl`, never typed ad hoc.
- `.terraform/terraform.tfstate` is the local backend-configuration cache, not
  the infrastructure state; the state itself exists only in the blob.
- State is never committed (`*.tfstate` in `.gitignore`).

## Alternatives considered
- Dedicated storage account (bootstrap script prepared in `infra/bootstrap/`):
  cleaner isolation, rejected to avoid extra Azure objects.
- Local state: rejected; CI could not plan and the state could be lost.
- HCP Terraform: rejected; another external account to maintain.

## Evidence
- Blob `omnichannel.tfstate` present in the container next to project 1 states;
  no `terraform.tfstate` in `infra/terraform/`.

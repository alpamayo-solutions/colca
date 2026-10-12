# TPM manufacturer certificates

Endorsement key certificates chain to these certificates for a request to be
`tpm-attested` (node enrollment spec §6). Self-signed certificates are roots,
everything else is an intermediate. A site adds more with
`enrollment.tpm_roots` (a PEM file).

Fetched on 2026-10-09 over the manufacturers' own distribution points:

| File | Manufacturer | Source |
|---|---|---|
| `infineon.pem` | Infineon OPTIGA TPM | `https://pki.infineon.com/Optiga{Rsa,Ecc}RootCA{,2}/…crt` and every `Optiga{Rsa,Ecc}MfrCA0NN` intermediate published there (001–080) |
| `stmicro.pem` | STMicroelectronics | `http://secure.globalsign.com/cacert/` `gstpmroot`, `stmtpmekroot`, `stmtpmeccroot01`, `stmtpmekint01`–`07`, `stmtpmeccint01`–`03` |
| `nuvoton.pem` | Nuvoton | `https://www.nuvoton.com/security/NTC-TPM-EK-Cert/` Nuvoton TPM Root CA 1110, 1111, 2110, 2111 and NTC TPM EK Root CA 01, 02 |
| `amd.pem` | AMD fTPM | `https://ftpm.amd.com/pki/aia/23452201D41C5AB064032BD23F158FEF` (AMDTPM root) |
| `intel.pem` | Intel PTT | `https://upgrades.intel.com/content/CRL/ekcert/EKRootPublicKey.cer` |

Known gaps: the GlobalSign TPM ECC root (above STM's ECC root) is not
included, so STM ECC EK certificates do not verify yet; AMD and Intel
firmware TPMs usually hold no EK certificate in NV and need the per-CPU
intermediates from the vendor's online service, so they are mostly `tpm`, not
`tpm-attested`. Add such certificates with `enrollment.tpm_roots`.

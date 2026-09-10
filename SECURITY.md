# Security

Please do not report a vulnerability in a public issue.

Report it privately through GitHub instead: open the repository's **Security**
tab and choose **Report a vulnerability**. Only the maintainers see the report.

Include what you can of the following:

- the version or commit you tested,
- how to reproduce it, ideally as a minimal configuration or test,
- what an attacker gains, and from where they need to be (the local network of
  a deployment, a machine with an enrolled key, a person with a token, anyone).

We confirm receipt within five working days and keep you informed while we fix
it. Once a fix is released we publish an advisory and credit you, unless you
prefer not to be named.

## Supported versions

Security fixes go into the latest release. There are no long-term support
branches yet.

## Supply chain

Each release carries SBOMs in SPDX and CycloneDX format: one per platform for
the container image, and one for `colca-data-contracts`. The image and the
release packages come with signed build provenance and SBOM attestations:

```bash
gh attestation verify oci://ghcr.io/alpamayo-solutions/colca:<version> --repo alpamayo-solutions/colca
```

Every night, CI builds fresh SBOMs of the `latest` and `main` images, of the
latest release and of `main`, and scans them for known vulnerabilities.

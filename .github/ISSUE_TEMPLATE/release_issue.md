---
name: Tailpipe Release
about: Tailpipe Release
title: "Tailpipe v<INSERT_VERSION_HERE>"
labels: release
---

#### Changelog

[Tailpipe v<INSERT_VERSION_HERE> Changelog](https://github.com/turbot/tailpipe/blob/v<INSERT_VERSION_HERE>/CHANGELOG.md)

## Checklist

### Pre-release checks

- [ ] All PR acceptance test pass in `tailpipe`
- [ ] Update check is working
- [ ] Tailpipe Changelog updated and reviewed

### Release Tailpipe

- [ ] Merge the release PR
- [ ] Trigger the `01 - Tailpipe: Release` workflow. This will create the release build.

### Post-release checks

- [ ] Update Changelog in the Release page (copy and paste from CHANGELOG.md)
- [ ] Test Linux install script
- [ ] Test Homebrew install
- [ ] Release branch merged to `develop`
- [ ] Raise Changelog update to `tailpipe.io`, get it reviewed.
- [ ] Merge Changelog update to `tailpipe.io`.
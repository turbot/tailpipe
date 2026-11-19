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

- [ ] The release PR's page shows its checks passed and what changes
- [ ] Update check is working

### Release Tailpipe

- [ ] A pipelings release approver approves the release PR and merges it. The merge builds, tags, publishes the GitHub Release with its notes, updates Homebrew, writes the changelog page on tailpipe.io, starts the Linux distro test and merges `main` back into `develop`.

### Post-release checks

- [ ] Test Linux install script
- [ ] Test Homebrew install
- [ ] Check the changelog page is live on tailpipe.io
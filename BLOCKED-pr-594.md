## Kind: design

# PR #594: go-jose/v4 4.1.1 -> 4.1.4 is superseded by develop

**Feedback:** no review threads or verdict. The PR is `DIRTY` (conflicts with `develop` in `go.mod` and
`go.sum`) and its last CI run (an old one) has `Test (ubuntu-latest, file_source)` red.

**Why I cannot action it:** `develop` already pins `github.com/go-jose/go-jose/v4 v4.1.6`
(backport #636, CVE-2026-63209 / CVE-2026-116587). The only correct way to resolve the `go.mod` and
`go.sum` conflict is to keep develop's 4.1.6; taking this PR's 4.1.4 would be a downgrade. The merge would
leave the PR with no change against `develop`. Whether to close it or keep it is a person's call, so I
pushed nothing.

**Recommendation:** close the PR as superseded by #636 (develop is at v4.1.6). Dependabot will not
reopen a bump below the version already on the base.

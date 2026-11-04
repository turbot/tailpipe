## Kind: environment

# Issue #646: add-to-project / add-to-project is red on develop

## What I was doing
Finding why `Assign Issue to Project` fails on issues opened in turbot/tailpipe.

## Finding
The cause is outside this repo. `.github/workflows/31-add-issues-to-pipeling-issue-tracker.yaml`
is a thin caller: it passes only `issue_number` and `repository` to
`turbot/steampipe-workflows/.github/workflows/assign-issue-to-pipeling-issue-tracker.yml@main`.
That reusable workflow hard-codes `project-url: https://github.com/orgs/turbot/projects/31`
and uses the org secret `GH_PROJECT_PAT` (reached here by `secrets: inherit`).

- `Could not resolve to a ProjectV2 with the number 31` means project 31 was deleted,
  renumbered or moved, or `GH_PROJECT_PAT` expired or lost access to it. A run here
  cannot tell which: its token sees no org projects at all.
- The `Unexpected input(s) 'fields'` warning is also inside the reusable workflow
  (`fields:` is not an input of `actions/add-to-project@v1.0.2`). It is a warning only.
- It fails org-wide, not just here. turbot/steampipe has failed the same job since
  2026-10-22; tailpipe's last pass was 2025-12-15. The other callers (flowpipe, powerpipe,
  pipe-fittings, steampipe-plugin-sdk, steampipe-postgres-fdw) use the same file.

## Ruled out
- A change to this caller: it has no project URL or token input to correct.
- `continue-on-error` or removing the workflow: it hides the break or drops a wanted
  feature, so it is not a fix.

## Needs a person (org admin)
1. Check that https://github.com/orgs/turbot/projects/31 exists and is the intended tracker.
   If it moved, update `project-url` in turbot/steampipe-workflows.
2. Check that the org secret `GH_PROJECT_PAT` is valid, not expired, and has `project`
   scope and access to that project.
3. Optionally drop the unused `fields:` line in the reusable workflow.

Once fixed upstream, no change is needed in tailpipe; re-run the failed job to clear this issue.

# Judgement notes for runs on tailpipe

## Dependency updates

A dependency PR into `develop` that touches a shipped file (`go.mod`, `go.sum`, anything not in
the `notShipped` list in `.github/release.json`) needs a `Changelog:` line in its body, or the
`shipyard/changelog-line` check fails. This includes the PRs the fleet's monthly `dependencies`
chore opens. Write one or two sentences a person using Tailpipe can read, such as "Updated
Go dependencies to their latest compatible versions, with the latest bug and security fixes."
Name any dependency whose update changes behaviour a person can see. A PR that touches only
files in `notShipped` (for example the quarterly "Actions and Docker" PR, which edits
`.github/`) needs no line; leave it out rather than leaving it empty, because an empty line
fails the check.

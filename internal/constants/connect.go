package constants

import "time"

// InitFileMaxAge is the maximum age of an db init file before it is cleaned up
const InitFileMaxAge = 24 * time.Hour

// ArgNoInstall is the `tailpipe connect` flag that leaves the extension install commands out of the init script
const ArgNoInstall = "no-install"

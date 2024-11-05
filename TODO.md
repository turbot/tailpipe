# TODO

## sdk
- validation function to confirm the row types implement the required JSON tags
- if plugins will live longer than CLI instance - then we need a remove observer function to close the strea

## plugins

## cancellation
- implement/test cancellation scenarios - ensure context cancellation and interrupt catching works

##  config
- configure inbox location: per execution? single location?

## colection
- add table config to collect request

## parsing
- Handle 3 part names for parsing etc - impacts name functions and eval ctx
- Do we load files recusrively from config location 

## Errors
- How to handle: 
  - wait for execution fails
  - handleFileWatcherEvent:
    - convertJSONLToParquet 
    - FileNameToExecutionId
    - delete JSON file
  - error on plugin stream
  - nil event on plugin stream
## Return Codes
- How does top level exit when there is an error - look at FailOnError usage
- Define return codes and ensure they are always used
- How do other apps do it?

## Logging
- [x] Logging: getting logs from plugin etc., stripping duplicate preface and controlling log level

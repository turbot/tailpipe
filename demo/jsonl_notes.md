# JSONL

> Notes: The `devopsdays` GitHub audit logs provided by Matty are dated from 2024-04-10 - 2025-04-10
> Advise to run with --from 2024-04-01

## Minimal configuration

Sadly with these log sets this means we need to demo a transform off the bat as the timestamps are epoch / unix millis

/1000 required to convert from unix millis to unix seconds

```hcl
partition "my_github" "demo" {
  source "file"  {
    paths       = ["/Users/graza/tailpipe_data/devopsdays"]
    file_layout = `.json.gz`
  }
}

table "my_github" {
  format = format.jsonl.default
  column "tp_timestamp" {
      type   = "timestamp"
      transform = "to_timestamp(\"@timestamp\" / 1000)"
  }
}

```

- Explain automapping of fields

## Adding in A column with a source

This will add a new column to the data, using an existing columns value (rather than replacing it we get both)

```hcl
table "my_github" {
  format      = format.jsonl.default
  column "tp_timestamp" {
    type   = "timestamp"
    transform = "to_timestamp(\"@timestamp\" / 1000)"
  }

  column "document_id" {
    source = "_document_id"
    type   = "varchar"
  }
}

# partition as before
```

## Map fields

Can introduce `map_fields` to only output columns beginning with alphabetic letter

```hcl
table "my_github" {
  format = format.jsonl.default
  column "tp_timestamp" {
    type   = "timestamp"
    transform = "to_timestamp(\"@timestamp\" / 1000)"
  }

  column "document_id" {
    source = "_document_id"
    type   = "varchar"
  }

  map_fields = ["[a-z]*"]
}

# partition as before
```

This will filter out `@timestamp` & `_document_id` (which we no longer need as we have `tp_timestamp` & `document_id`)


## Reducing Dataset

We've got a lot of fields but most of them aren't all that useful to what we want to see, can reduce with `map_fields`

Additionally, `document_id` isn't useful, so we'll remove this column

```hcl
table "my_github" {
  format = format.jsonl.default
  column "tp_timestamp" {
    type   = "timestamp"
    transform = "to_timestamp(\"@timestamp\" / 1000)"
  }
 
  column "document_id" {
    source = "_document_id"
    type   = "varchar"
  }

  map_fields = [
    "action",
    "actor",
    "event",
    "org",
    "owner",
    "repo",
    "team",
    "user",
    "visibility"
  ]
}

# partition as before
```

## Using transform to populate tp_ field(s)

```hcl
table "my_github" {
  format = format.jsonl.default
  column "tp_timestamp" {
    type   = "timestamp"
    transform = "to_timestamp(\"@timestamp\" / 1000)"
  }

  column "document_id" {
    source = "_document_id"
    type   = "varchar"
  }
  
  column "tp_usernames" {
    transform = "array[actor, user, owner]"
  }

  map_fields = [
    "action",
    "actor",
    "event",
    "org",
    "owner",
    "repo",
    "team",
    "user",
    "visibility"
  ]
}


```

This is nice, but gives `nil` or duplicate values, we can get more complex to refine!

```hcl
table "my_github" {
  format = format.jsonl.default
  column "tp_timestamp" {
    type   = "timestamp"
    transform = "to_timestamp(\"@timestamp\" / 1000)"
  }

  column "document_id" {
    source = "_document_id"
    type   = "varchar"
  }

  column "tp_usernames" {
    transform = "list_distinct(list_filter(array[actor, user, owner], x->x is not null))"
  }

  map_fields = [
    "action",
    "actor",
    "event",
    "org",
    "owner",
    "repo",
    "team",
    "user",
    "visibility"
  ]
}

```


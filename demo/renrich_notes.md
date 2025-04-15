# Row Enrichment

Using OpenStack Syslogs

> Note: actual syslogs don't have a year `Apr 08 12:34:56` but I had these converted to using ISO8601 to avoid strptime complexities in basic example

## Basic Starting Point

Most simplistic approach

```hcl
format "regex" "example" {
  layout = `^(?P<timestamp>\S+) (?P<host>\S+) (?P<service>\w+)\[(?P<pid>\d+)\]: \[(?P<level>[A-Z]+)\] (?P<message>.+)$`
}

table "demo2" {
  column "tp_timestamp" {
    source = "timestamp"
  }
}

partition "demo2" "openstack_syslogs" {
  source "file"  {
    format      = format.regex.example
    paths       = ["/Users/graza/tailpipe_data/openstack"]
    file_layout = `.log`
  }
}
```

fields are auto-mapped, we get all

```
> .inspect demo2
Column              Type
host                varchar
level               varchar
message             varchar
pid                 varchar
service             varchar
timestamp           varchar
tp_...
```

## changing a type

Column `pid` currently shows as a varchar but we know this is an integer value

```hcl
# format as before

table "demo2" {
  column "tp_timestamp" {
    source = "timestamp"
  }

  column "pid" {
    type = "integer"
  }
}

# partition as before
```

```
> .inspect demo2
Column              Type
host                varchar
level               varchar
message             varchar
pid                 integer
service             varchar
timestamp           varchar
tp_...
```

**Alternatively timestamp->timestamp**

```hcl
# format as before

table "demo2" {
  column "tp_timestamp" {
    source = "timestamp"
  }

  column "timestamp" {
    type = "timestamp"
  }
}

# partition as before
```

## using a transform for enrichment (tp_ fields)

```hcl
# format as before
table "demo2" {
  column "tp_timestamp" {
    source = "timestamp"
  }

  column "pid" {
    type = "integer"
  }

  column "tp_akas" {
    transform = "ARRAY[service, host]"
    type      = "varchar[]"
  }
}
# partition as before
```

## adding a new field using a transform 

concatenating `host/service`

```hcl
# format as before
table "demo2" {
  column "tp_timestamp" {
    source = "timestamp"
  }

  column "pid" {
    type = "integer"
  }

  column "tp_akas" {
    transform = "ARRAY[service, host]"
    type      = "varchar[]"
  }

  column "host_service" {
    transform = "host || '/' || service"
    type      = "varchar"
  }
}
# partition as before
```

```
> select distinct(host_service) from demo2;
+-----------------------+
| host_service          |
+-----------------------+
| controller-1/cinder   |
| controller-3/neutron  |
| controller-3/keystone |
| controller-2/nova     |
| controller-3/nova     |
| controller-1/neutron  |
| controller-3/cinder   |
| controller-1/nova     |
| controller-2/cinder   |
| controller-2/glance   |
| controller-2/neutron  |
| controller-2/keystone |
| controller-1/keystone |
| controller-1/glance   |
| controller-3/glance   |
+-----------------------+
```

## using map_fields to reduce dataset

perhaps only interested in `level`, `message` and `timestamp` (stored in `tp_timestamp`)

adjust as follows:

```hcl
# format as before
table "demo2" {
  column "tp_timestamp" {
    source = "timestamp"
  }

  map_fields = ["message", "level"]
}
# partition as before
```

> note: we still get the tp_field columns


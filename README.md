# AWS S3 Exporter

This exporter provides metrics for AWS S3 bucket objects by querying the API with a given bucket and constructing metrics based on the returned objects.

I find it useful for ensuring that backup jobs and batch uploads are functioning by comparing the growth in size/number of objects over time, or comparing the last modified date to an expected value.

## Building

```
make
```

## Running

```
./cds_s3_exporter <flags>
```

You can query a bucket by supplying them as parameters to /inspect:

```
curl localhost:9340/inspect?bucket=some-bucket
```

### AWS Credentials

The exporter creates an AWS session without any configuration. You must specify credentials yourself as documented [here](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html).

Remember, if you want to load credentials from `~/.aws/config` then you need to to set:

```
export AWS_SDK_LOAD_CONFIG=true
```

### Docker

```
docker pull ashokhin/cds-s3-exporter
```

You will need to supply AWS credentials to the container, as mentioned in the previous section, either by setting the appropriate environment variables with `-e`, or by mounting your `~/.aws/` directory with `-v`.

```
# Environment variables
docker run -p 9340:9340 -e AWS_ACCESS_KEY_ID=<value> -e AWS_SECRET_ACCESS_KEY=<value> -e AWS_REGION=<value> cds-s3-exporter:latest <flags>
# Mounted volume
docker run -p 9340:9340 -e AWS_SDK_LOAD_CONFIG=true -e HOME=/ -v $HOME/.aws:/.aws cds-s3-exporter:latest <flags>
```

## Flags

```
  -h, --help              Show context-sensitive help (also try --help-long and --help-man).
      --web.listen-address=":9340"
                          Address to listen on for web interface and telemetry.
      --web.metrics-path="/metrics"
                          Path under which to expose metrics
      --web.inspect-path="/inspect"
                          Path under which to expose the inspect endpoint
      --exporter-config-file="./config.yml"
                          Path to exporter config file
      --log.level="info"  Only log messages with the given severity or above. Valid levels: [debug, info, warn, error, fatal]
      --log.format="logger:stderr"
                          Set the log target and format. Example: "logger:syslog?appname=bob&local=7" or "logger:stdout?json=true"
      --version           Show application version.
```

Flags can also be set as environment variables, prefixed by `cds_s3_exporter_`. For example: `cds_s3_exporter_S3_ENDPOINT_URL=http://s3.example.local`.

## Metrics

| Metric                                 | Meaning                                                              | Labels               |
| -------------------------------------- | -------------------------------------------------------------------- | -------------------- |
| cds_s3_biggest_object_size_bytes       | The size of the largest object.                                      | bucket               |
| cds_s3_last_modified_object_date       | The modification date of the most recently modified object.          | bucket               |
| cds_s3_last_modified_object_size_bytes | The size of the object that was modified most recently.              | bucket               |
| cds_s3_list_duration_seconds           | The duration of the ListObjects operation                            | bucket               |
| cds_s3_list_success                    | Did the ListObjects operation complete successfully?                 | bucket               |
| cds_s3_objects_size_sum_bytes          | The sum of the size of all the objects.                              | bucket               |
| cds_s3_objects                         | The total number of objects.                                         | bucket               |
| cds_s3_cds_objects_size_sum_bytes      | The sum of the size of all the objects by sin and modification date. | bucket, moddate, sin |
| cds_s3_cds_objects_total               | The total number of objects  by sin and modification date.           | bucket, moddate, sin |

## Prometheus

### Configuration

You can pass the params to a single instance of the exporter using relabelling, like so:

```yml
scrape_configs:
  - job_name: "s3"
    metrics_path: /inspect
    static_configs:
      - targets:
          - bucket=stuff;
          - bucket=other-stuff;
    relabel_configs:
      - source_labels: [__address__]
        regex: "^bucket=(.*);$"
        replacement: "${1}"
        target_label: "__param_bucket"
      - target_label: __address__
        replacement: 127.0.0.1:9340 # S3 exporter.
```

### Example Queries

Return series where the last modified object date is more than 24 hours ago:

```
(time() - cds_s3_last_modified_object_date) / 3600 > 24
```

# AWS S3 Exporter

Этот экспортер предоставляет метрики для AWS S3-совместимых объектах в определённых бакетах.
Список баккетов передаётся через конигурационный YAML-файл.
Пример конфигурации указан в файле `config_example.yml`.

## Building

```
make
```

## Running

```
./s3_exporter <flags>
```

### AWS Credentials

Экспортер создаёт сессиюю с S3-хранилищем используя точку входа и access_key + secret_key из конфигурационного YAML-файла.
Пример конфигурации указан в файле `config_example.yml`.

## Flags

```
  -h, --help              Show context-sensitive help (also try --help-long and --help-man).
      --web.listen-address=":9340"
                          Address to listen on for web interface and telemetry.
      --web.metrics-path="/metrics"
                          Path under which to expose metrics
      --exporter-config-file="./config.yml"
                          Path to exporter config file
      --log.level="info"  Only log messages with the given severity or above. Valid levels: [debug, info, warn, error, fatal]
      --log.format="logger:stderr"
                          Set the log target and format. Example: "logger:syslog?appname=bob&local=7" or "logger:stdout?json=true"
      --version           Show application version.
```

Flags can also be set as environment variables, prefixed by `s3_exporter_`. For example: `s3_exporter_S3_ENDPOINT_URL=http://s3.example.local`.

## Metrics

| Metric                                 | Meaning                                                                       | Labels                        |
| -------------------------------------- | ----------------------------------------------------------------------------- | ----------------------------- |
| s3_biggest_object_size_bytes           | The size of the largest object.                                               | bucket                        |
| s3_last_modified_object_date           | The modification date of the most recently modified object.                   | bucket                        |
| s3_last_modified_object_size_bytes     | The size of the object that was modified most recently.                       | bucket                        |
| s3_list_duration_seconds               | The duration of the ListObjects operation                                     | bucket                        |
| s3_list_success                        | Did the ListObjects operation complete successfully?                          | bucket                        |
| s3_objects_size_sum_bytes              | The sum of the size of all the objects.                                       | bucket                        |
| s3_objects                             | The total number of objects.                                                  | bucket                        |
| s3_cds_objects_size_sum_bytes          | The sum of the size of all the objects by sin and modification date.          | bucket, moddate, sin          |
| s3_cds_objects_total                   | The total number of objects by sin and modification date.                     | bucket, moddate, sin          |
| s3_trigger_objects_size_sum_bytes      | The sum of the size of all the objects by sin, modification date and program. | bucket, moddate, sin, program |
| s3_trigger_objects_total               | The total number of objectsby sin, modification date and program.             | bucket, moddate, sin, program |

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

Возвраащает серию, где последняя дата модификации объекта больше 24 часов:
```
(time() - s3_last_modified_object_date) / 3600 > 24
```

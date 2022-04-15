package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v3"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

const (
	namespace = "s3"
)

var (
	s3ListSuccess = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "list_success"),
		"If the ListObjects operation was a success",
		[]string{"bucket"}, nil,
	)
	s3ListDuration = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "list_duration_seconds"),
		"The total duration of the list operation",
		[]string{"bucket"}, nil,
	)
	s3LastModifiedObjectDate = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "last_modified_object_date"),
		"The last modified date of the object that was modified most recently",
		[]string{"bucket"}, nil,
	)
	s3LastModifiedObjectSize = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "last_modified_object_size_bytes"),
		"The size of the object that was modified most recently",
		[]string{"bucket"}, nil,
	)
	s3ObjectTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "objects_total"),
		"The total number of objects for bucket",
		[]string{"bucket"}, nil,
	)
	s3SumSize = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "objects_size_sum_bytes"),
		"The total size of all objects summed",
		[]string{"bucket"}, nil,
	)
	s3BiggestSize = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "biggest_object_size_bytes"),
		"The size of the biggest object",
		[]string{"bucket"}, nil,
	)
	s3CDSObjectTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "cds_objects_total"),
		"The total number of all objects for bucket",
		[]string{"bucket", "moddate", "sin", "type"}, nil,
	)
	s3CDSSize = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "cds_objects_size_sum_bytes"),
		"The total size of all objects summed by sin and modification date",
		[]string{"bucket", "moddate", "sin", "type"}, nil,
	)
	s3TriggerTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "trigger_objects_total"),
		"The total number of all objects for bucket",
		[]string{"bucket", "moddate", "sin", "program"}, nil,
	)
	s3TriggerSize = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "trigger_objects_size_sum_bytes"),
		"The total size of all objects summed by sin, program and modification date",
		[]string{"bucket", "moddate", "sin", "program"}, nil,
	)
)

type Config struct {
	EndpointUrl    string   `yaml:"endpoint_url"`
	AwsAccessKey   string   `yaml:"access_key"`
	AwsSecretKey   string   `yaml:"secret_key"`
	DisableSSL     bool     `yaml:"disable_ssl"`
	CdsBuckets     []string `yaml:"cds_buckets"`
	TriggerBuckets []string `yaml:"trigger_buckets"`
	Timezone       string   `yaml:"timezone"`
}

func (conf *Config) setDefaults() {
	conf.EndpointUrl = "127.0.0.1:8080"
	conf.Timezone = "UTC"
	conf.DisableSSL = false
}

// Exporter is our exporter type
type Exporter struct {
	conf   Config
	svc    s3iface.S3API
	logger log.Logger
}

// Describe all the metrics we export
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- s3ListSuccess
	ch <- s3ListDuration
	ch <- s3LastModifiedObjectDate
	ch <- s3LastModifiedObjectSize
	ch <- s3ObjectTotal
	ch <- s3SumSize
	ch <- s3BiggestSize
	ch <- s3CDSObjectTotal
}

// Collect metrics
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	level.Debug(e.logger).Log("msg", "start collect")
	type CdsObject struct {
		ObjectSin  string
		ModifyDate string
		FileType   string
		BucketName string
	}

	type TriggerObject struct {
		ObjectSin     string
		ObjectProgram string
		ModifyDate    string
	}

	var (
		lastModified      time.Time
		numberOfObjects   float64
		totalSize         int64
		biggestObjectSize int64
		lastObjectSize    int64
	)

	logger := e.logger

	// Set timezone for file modification
	timezone, _ := time.LoadLocation(e.conf.Timezone)
	// Start processing cds objects
	// Create regexp template
	reCdsPref := regexp.MustCompile(`^[a-z0-9/]+_`)
	reCdsSuff := regexp.MustCompile(`_.+\.[cg]ds$`)
	reSinMatch := regexp.MustCompile(`^\d{3,5}$`)
	// Continue making requests until we've listed and compared the date of every object

	startList := time.Now()
	for _, cdsBucket := range e.conf.CdsBuckets {
		cdsSize := make(map[CdsObject]int64)
		cdsCount := make(map[CdsObject]uint16)

		// Create query
		query := &s3.ListObjectsV2Input{
			Bucket: aws.String(cdsBucket),
		}

		for {
			resp, err := e.svc.ListObjectsV2(query)
			if err != nil {
				level.Debug(logger).Log("msg", "failed when listing s3 objects", "error", err.Error())
				ch <- prometheus.MustNewConstMetric(
					s3ListSuccess, prometheus.GaugeValue, 0, cdsBucket,
				)
				return
			}
			for _, item := range resp.Contents {
				objectName := strings.ToLower(*item.Key)
				sin := reCdsPref.ReplaceAllString(objectName, ``)
				sin = reCdsSuff.ReplaceAllString(sin, ``)
				if !reSinMatch.MatchString(sin) {
					level.Debug(logger).Log("msg", "SIN of object doesn't match regexp and will be skipped", "object", objectName, "sin", sin, "regexp", reSinMatch)
					continue
				}
				t := *item.LastModified
				t = t.In(timezone)
				level.Debug(logger).Log("msg", "shifting time for object", "object", objectName, "shift_from", *item.LastModified, "shift_to", t)
				modDate := t.Format("2006.01.02")
				fileType := objectName[len(objectName)-3:]
				cdsSize[CdsObject{sin, modDate, fileType, cdsBucket}] += *item.Size
				cdsCount[CdsObject{sin, modDate, fileType, cdsBucket}] += 1
				numberOfObjects++
				totalSize = totalSize + *item.Size
				if item.LastModified.After(lastModified) {
					lastModified = *item.LastModified
					lastObjectSize = *item.Size
				}
				if *item.Size > biggestObjectSize {
					biggestObjectSize = *item.Size
				}
			}
			if resp.NextContinuationToken == nil {
				level.Debug(logger).Log("msg", "all objects has been listed")
				break
			}
			query.ContinuationToken = resp.NextContinuationToken
		}

		level.Debug(logger).Log("msg", "CDS size", "value", cdsSize)
		level.Debug(logger).Log("msg", "CDS count", "value", cdsCount)

		listDuration := time.Since(startList).Seconds()

		ch <- prometheus.MustNewConstMetric(
			s3ListSuccess, prometheus.GaugeValue, 1, cdsBucket,
		)
		ch <- prometheus.MustNewConstMetric(
			s3ListDuration, prometheus.GaugeValue, listDuration, cdsBucket,
		)
		ch <- prometheus.MustNewConstMetric(
			s3LastModifiedObjectDate, prometheus.GaugeValue, float64(lastModified.UnixNano()/1e9), cdsBucket,
		)
		ch <- prometheus.MustNewConstMetric(
			s3LastModifiedObjectSize, prometheus.GaugeValue, float64(lastObjectSize), cdsBucket,
		)
		ch <- prometheus.MustNewConstMetric(
			s3ObjectTotal, prometheus.GaugeValue, numberOfObjects, cdsBucket,
		)
		ch <- prometheus.MustNewConstMetric(
			s3BiggestSize, prometheus.GaugeValue, float64(biggestObjectSize), cdsBucket,
		)
		ch <- prometheus.MustNewConstMetric(
			s3SumSize, prometheus.GaugeValue, float64(totalSize), cdsBucket,
		)

		// Send all collected metrics in Prometheus registry
		for key, value := range cdsSize {
			level.Debug(logger).Log("msg", "CDS size metric send into Prometheus registry", "sin", key.ObjectSin, "date", key.ModifyDate, "type", key.FileType, "value", value)
			ch <- prometheus.MustNewConstMetric(
				s3CDSSize, prometheus.GaugeValue, float64(value), key.BucketName, key.ModifyDate, key.ObjectSin, key.FileType,
			)
		}

		for key, value := range cdsCount {
			level.Debug(logger).Log("msg", "CDS count metric send into Prometheus registry", "sin", key.ObjectSin, "date", key.ModifyDate, "type", key.FileType, "value", value)
			ch <- prometheus.MustNewConstMetric(
				s3CDSObjectTotal, prometheus.GaugeValue, float64(value), key.BucketName, key.ModifyDate, key.ObjectSin, key.FileType,
			)
		}

	}

	// Start processing trigger objects
	reTrigPref := regexp.MustCompile(`^[a-z0-9/]+_`)
	for _, triggerBucket := range e.conf.TriggerBuckets {
		triggerSize := make(map[TriggerObject]int64)
		triggerCount := make(map[TriggerObject]uint16)
		// Create query
		query := &s3.ListObjectsV2Input{
			Bucket: &triggerBucket,
		}
		startList = time.Now()
		for {
			resp, err := e.svc.ListObjectsV2(query)
			if err != nil {
				level.Error(logger).Log("msg", "failed list s3 objects", "error", err.Error())
				ch <- prometheus.MustNewConstMetric(
					s3ListSuccess, prometheus.GaugeValue, 0, triggerBucket,
				)
				return
			}
			for _, item := range resp.Contents {
				objectName := strings.ToLower(*item.Key)
				objectNamePart := reTrigPref.ReplaceAllString(objectName, ``)
				objectNameSlice := strings.Split(objectNamePart, `_`)
				sin := objectNameSlice[0]
				if !reSinMatch.MatchString(sin) {
					level.Debug(logger).Log("msg", "SIN in trigger object name doesn't match regexp and will be skipped", "object", objectName, "sin", sin, "regexp", reSinMatch)
					continue
				}
				program := strings.ToUpper(objectNameSlice[1])
				// Parse string (part of name) to timestamp
				t, err := time.Parse("200601021504", objectNameSlice[2][0:12])
				if err != nil {
					level.Debug(logger).Log("msg", "failed to parse time string", "object", objectName, "timestring", objectNameSlice[2][0:12], "error", err.Error())
					continue
				}
				t = t.In(timezone)
				level.Debug(logger).Log("msg", "shifting time for object", "object", objectName, "shift_from", *item.LastModified, "shift_to", t)
				modDate := t.Format("2006.01.02")
				triggerSize[TriggerObject{sin, program, modDate}] += *item.Size
				triggerCount[TriggerObject{sin, program, modDate}] += 1
				numberOfObjects++
				totalSize = totalSize + *item.Size
				if item.LastModified.After(lastModified) {
					lastModified = *item.LastModified
					lastObjectSize = *item.Size
				}
				if *item.Size > biggestObjectSize {
					biggestObjectSize = *item.Size
				}
			}
			if resp.NextContinuationToken == nil {
				break
			}
			query.ContinuationToken = resp.NextContinuationToken
		}

		listDuration := time.Since(startList).Seconds()

		// Send all collected metrics in Prometheus registry
		for key, value := range triggerSize {
			level.Debug(logger).Log("msg", "expose new metric about triggers size", "sin", key.ObjectSin, "program", key.ObjectProgram, "date", key.ModifyDate, "value", value)
			ch <- prometheus.MustNewConstMetric(
				s3TriggerSize, prometheus.GaugeValue, float64(value), triggerBucket, key.ModifyDate, key.ObjectSin, key.ObjectProgram,
			)
		}

		for key, value := range triggerCount {
			ch <- prometheus.MustNewConstMetric(
				s3TriggerTotal, prometheus.GaugeValue, float64(value), triggerBucket, key.ModifyDate, key.ObjectSin, key.ObjectProgram,
			)
		}

		ch <- prometheus.MustNewConstMetric(
			s3ListSuccess, prometheus.GaugeValue, 1, triggerBucket,
		)
		ch <- prometheus.MustNewConstMetric(
			s3ListDuration, prometheus.GaugeValue, listDuration, triggerBucket,
		)
		ch <- prometheus.MustNewConstMetric(
			s3LastModifiedObjectDate, prometheus.GaugeValue, float64(lastModified.UnixNano()/1e9), triggerBucket,
		)
		ch <- prometheus.MustNewConstMetric(
			s3LastModifiedObjectSize, prometheus.GaugeValue, float64(lastObjectSize), triggerBucket,
		)
		ch <- prometheus.MustNewConstMetric(
			s3ObjectTotal, prometheus.GaugeValue, numberOfObjects, triggerBucket,
		)
		ch <- prometheus.MustNewConstMetric(
			s3BiggestSize, prometheus.GaugeValue, float64(biggestObjectSize), triggerBucket,
		)
		ch <- prometheus.MustNewConstMetric(
			s3SumSize, prometheus.GaugeValue, float64(totalSize), triggerBucket,
		)
	}
}

func metricsHandler(w http.ResponseWriter, r *http.Request, svc s3iface.S3API, conf Config, logger log.Logger) {
	if len(conf.CdsBuckets) == 0 {
		http.Error(w, "bucket parameter is missing", http.StatusBadRequest)
		return
	}

	exporter := &Exporter{
		conf:   conf,
		svc:    svc,
		logger: logger,
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(exporter)

	// Serve
	h := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	h.ServeHTTP(w, r)
}

func init() {
	prometheus.MustRegister(version.NewCollector(namespace + "_exporter"))
}

func readFile(confFile string, cfg *Config, logger log.Logger) {
	f, err := os.Open(confFile)

	if err != nil {
		level.Error(logger).Log("msg", "failed to open file", "file", confFile, "error", err.Error())
	}

	defer f.Close()

	decoder := yaml.NewDecoder(f)

	if err := decoder.Decode(cfg); err != nil {
		level.Error(logger).Log("msg", "failed to docode YAML config", "error", err.Error())
		os.Exit(1)
	}
}

func main() {
	var (
		listenAddress = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9340").String()
		metricsPath   = kingpin.Flag("web.metrics-path", "Path under which to expose metrics").Default("/metrics").String()
		expConfigPath = kingpin.Flag("exporter-config-file", "Path to exporter config file").Default("./config.yml").String()
	)

	promlogConfig := &promlog.Config{}
	flag.AddFlags(kingpin.CommandLine, promlogConfig)
	kingpin.Version(version.Print(namespace + "_exporter"))
	kingpin.CommandLine.UsageWriter(os.Stdout)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()
	logger := promlog.New(promlogConfig)
	var sess *session.Session
	var err error
	var exporterConfig Config

	var forcePath bool = true
	var awsRegion string = "us-west-1"

	*expConfigPath, _ = filepath.Abs(*expConfigPath)
	// Fil with defaults
	exporterConfig.setDefaults()
	level.Debug(logger).Log("msg", "exporter config with defaults", "value", fmt.Sprintf("%+v", exporterConfig))
	// Load from config
	readFile(*expConfigPath, &exporterConfig, logger)
	level.Debug(logger).Log("msg", "exporter's config from file", "file", *expConfigPath, "value", fmt.Sprintf("%+v", exporterConfig))
	// Validate timezone
	_, err = time.LoadLocation(exporterConfig.Timezone)

	if err != nil {
		level.Error(logger).Log("msg", "failed to load timezone", "timezone", exporterConfig.Timezone, "error", err.Error())
		os.Exit(1)
	} else {
		level.Info(logger).Log("msg", "load timezone", "timezone", exporterConfig.Timezone)
	}

	awsCreds := credentials.NewStaticCredentials(exporterConfig.AwsAccessKey, exporterConfig.AwsSecretKey, "")
	sess, err = session.NewSession()

	if err != nil {
		level.Error(logger).Log("msg", "error creating session", "error", err.Error())
		os.Exit(1)
	}

	cfg := aws.NewConfig()

	cfg.WithCredentials(awsCreds)
	cfg.WithRegion(awsRegion)
	cfg.WithEndpoint(exporterConfig.EndpointUrl)
	cfg.WithDisableSSL(exporterConfig.DisableSSL)
	cfg.WithS3ForcePathStyle(forcePath)

	svc := s3.New(sess, cfg)

	level.Info(logger).Log("msg", "starting exporter", "version", version.Info())
	level.Info(logger).Log("msg", "Build context", version.BuildContext())

	http.HandleFunc(*metricsPath, func(w http.ResponseWriter, r *http.Request) {
		metricsHandler(w, r, svc, exporterConfig, logger)
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
						 <head><title>AWS CDS S3 Exporter</title></head>
						 <body>
						 <h1>AWS CDS S3 Exporter</h1>
						 <p><a href='` + *metricsPath + `'>Metrics</a></p>
						 </body>
						 </html>`))
	})

	level.Info(logger).Log("msg", "listening on address", "address", *listenAddress)
	if err := http.ListenAndServe(*listenAddress, nil); err != nil {
		level.Error(logger).Log("msg", "error running HTTP server", "err", err.Error())
		os.Exit(1)
	}
}

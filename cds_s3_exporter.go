package main

import (
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
	yaml "gopkg.in/yaml.v2"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

const (
	namespace = "cds_s3"
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
	CdsBucket      string   `yaml:"cds_bucket"`
	TriggerBuckets []string `yaml:"trigger_buckets"`
	Timezone       string   `yaml:"timezone"`
}

func (conf *Config) set_defaults() {
	if conf.EndpointUrl == "" {
		conf.EndpointUrl = "127.0.0.1:8080"
	}
	if conf.Timezone == "" {
		conf.Timezone = "Europe/Moscow"
	}
}

// Exporter is our exporter type
type Exporter struct {
	conf Config
	svc  s3iface.S3API
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
	type CdsObject struct {
		ObjectSin  string
		ModifyDate string
		FileType   string
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
	cdsSize := make(map[CdsObject]int64)
	cdsCount := make(map[CdsObject]uint16)
	triggerSize := make(map[TriggerObject]int64)
	triggerCount := make(map[TriggerObject]uint16)

	// Create query
	query := &s3.ListObjectsV2Input{
		Bucket: &e.conf.CdsBucket,
	}
	// Set timezone for file modification
	timezone, _ := time.LoadLocation(e.conf.Timezone)
	// Start processing cds objects
	// Create regexp template
	reCdsPref := regexp.MustCompile(`^[a-z0-9/]+_`)
	reCdsSuff := regexp.MustCompile(`_.+\.[cg]ds$`)
	reSinMatch := regexp.MustCompile(`^\d{3,5}$`)
	// Continue making requests until we've listed and compared the date of every object
	startList := time.Now()
	for {
		resp, err := e.svc.ListObjectsV2(query)
		if err != nil {
			log.Errorln(err)
			ch <- prometheus.MustNewConstMetric(
				s3ListSuccess, prometheus.GaugeValue, 0, e.conf.CdsBucket,
			)
			return
		}
		for _, item := range resp.Contents {
			objectName := strings.ToLower(*item.Key)
			if !(strings.Contains(objectName, "/")) {
				log.Debugf("Object '%v' not match. Skip.\n", objectName)
				continue
			}
			sin := reCdsPref.ReplaceAllString(objectName, ``)
			sin = reCdsSuff.ReplaceAllString(sin, ``)
			matched := reSinMatch.MatchString(sin)
			if !matched {
				log.Debugf("SIN '%v' in object: '%v' not match. Skip.\n", sin, objectName)
				continue
			}
			t := *item.LastModified
			t = t.In(timezone)
			log.Debugf("Shift time '%v' to '%v' for object '%v'\n", *item.LastModified, t, objectName)
			modDate := t.Format("2006.01.02")
			fileType := objectName[len(objectName)-3:]
			cdsSize[CdsObject{sin, modDate, fileType}] += *item.Size
			cdsCount[CdsObject{sin, modDate, fileType}] += 1
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
	for key, value := range cdsSize {
		log.Debugf("SIN: %v, Date: %v, Type: %v, Value: %v\n", key.ObjectSin, key.ModifyDate, key.FileType, value)
		ch <- prometheus.MustNewConstMetric(
			s3CDSSize, prometheus.GaugeValue, float64(value), e.conf.CdsBucket, key.ModifyDate, key.ObjectSin, key.FileType,
		)
	}

	for key, value := range cdsCount {
		ch <- prometheus.MustNewConstMetric(
			s3CDSObjectTotal, prometheus.GaugeValue, float64(value), e.conf.CdsBucket, key.ModifyDate, key.ObjectSin, key.FileType,
		)
	}

	ch <- prometheus.MustNewConstMetric(
		s3ListSuccess, prometheus.GaugeValue, 1, e.conf.CdsBucket,
	)
	ch <- prometheus.MustNewConstMetric(
		s3ListDuration, prometheus.GaugeValue, listDuration, e.conf.CdsBucket,
	)
	ch <- prometheus.MustNewConstMetric(
		s3LastModifiedObjectDate, prometheus.GaugeValue, float64(lastModified.UnixNano()/1e9), e.conf.CdsBucket,
	)
	ch <- prometheus.MustNewConstMetric(
		s3LastModifiedObjectSize, prometheus.GaugeValue, float64(lastObjectSize), e.conf.CdsBucket,
	)
	ch <- prometheus.MustNewConstMetric(
		s3ObjectTotal, prometheus.GaugeValue, numberOfObjects, e.conf.CdsBucket,
	)
	ch <- prometheus.MustNewConstMetric(
		s3BiggestSize, prometheus.GaugeValue, float64(biggestObjectSize), e.conf.CdsBucket,
	)
	ch <- prometheus.MustNewConstMetric(
		s3SumSize, prometheus.GaugeValue, float64(totalSize), e.conf.CdsBucket,
	)

	// Start processing trigger objects
	reTrigPref := regexp.MustCompile(`^[a-z0-9]+/`)
	for _, triggerBucket := range e.conf.TriggerBuckets {
		triggerSize = make(map[TriggerObject]int64)
		triggerCount = make(map[TriggerObject]uint16)
		// Create query
		query = &s3.ListObjectsV2Input{
			Bucket: &triggerBucket,
		}
		startList = time.Now()
		for {
			resp, err := e.svc.ListObjectsV2(query)
			if err != nil {
				log.Errorln(err)
				ch <- prometheus.MustNewConstMetric(
					s3ListSuccess, prometheus.GaugeValue, 0, triggerBucket,
				)
				return
			}
			for _, item := range resp.Contents {
				objectName := strings.ToLower(*item.Key)
				if !(strings.Contains(objectName, "/")) {
					log.Debugf("Object '%v' not match. Skip.", objectName)
					continue
				}
				objectNamePart := reTrigPref.ReplaceAllString(objectName, ``)
				objectNameSlice := strings.Split(objectNamePart, `_`)
				sin := objectNameSlice[0]
				matched := reSinMatch.MatchString(sin)
				if !matched {
					log.Debugf("SIN '%v' in trigger object: '%v' not match. Skip.", sin, objectName)
					continue
				}
				program := strings.ToUpper(objectNameSlice[1])
				// Parse string (part of name) to timestamp
				t, err := time.Parse("200601021504", objectNameSlice[2][0:12])
				if err != nil {
					log.Debugf("Time string '%v' cannot be parsed to timestamp with error: '%v'", objectNameSlice[2][0:12], err)
					continue
				}
				t = t.In(timezone)
				log.Debugf("Shift time '%v' to '%v' for object '%v'", *item.LastModified, t, objectName)
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

		listDuration = time.Since(startList).Seconds()

		// Send all collected metrics in Prometheus registry
		for key, value := range triggerSize {
			log.Debugf("SIN: '%v', Program: '%v', Date: '%v', Value: '%v'\n", key.ObjectSin, key.ObjectProgram, key.ModifyDate, value)
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

func metricsHandler(w http.ResponseWriter, r *http.Request, svc s3iface.S3API, conf Config) {
	if conf.CdsBucket == "" {
		http.Error(w, "bucket parameter is missing", http.StatusBadRequest)
		return
	}

	exporter := &Exporter{
		conf: conf,
		svc:  svc,
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

func readFile(confFile string, cfg *Config) {
	f, err := os.Open(confFile)
	if err != nil {
		log.Errorln(err)
		os.Exit(2)
	}
	defer f.Close()

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(cfg)
	if err != nil {
		log.Errorln(err)
		os.Exit(2)
	}
}

func main() {
	var (
		app           = kingpin.New(namespace+"_exporter", "Export metrics for CDS S3-compatible storage").DefaultEnvars()
		listenAddress = app.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9340").String()
		metricsPath   = app.Flag("web.metrics-path", "Path under which to expose metrics").Default("/metrics").String()
		expConfigPath = app.Flag("exporter-config-file", "Path to exporter config file").Default("./config.yml").String()
	)

	log.AddFlags(app)
	app.Version(version.Print(namespace + "_exporter"))
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	var sess *session.Session
	var err error
	var exporterConfig Config

	var forcePath bool = true
	var awsRegion string = "us-west-1"

	readFile(*expConfigPath, &exporterConfig)
	// Fil to default when not configured in YAML
	exporterConfig.set_defaults()
	// Validate timezone
	_, err = time.LoadLocation(exporterConfig.Timezone)
	if err != nil {
		log.Errorln("Invalid timezone:", exporterConfig.Timezone)
		os.Exit(2)
	} else {
		log.Infoln("Load timezone", exporterConfig.Timezone)
	}
	awsCreds := credentials.NewStaticCredentials(exporterConfig.AwsAccessKey, exporterConfig.AwsSecretKey, "")
	sess, err = session.NewSession()
	if err != nil {
		log.Errorln("Error creating sessions ", err)
	}

	cfg := aws.NewConfig()

	cfg.WithCredentials(awsCreds)
	cfg.WithRegion(awsRegion)
	cfg.WithEndpoint(exporterConfig.EndpointUrl)
	cfg.WithDisableSSL(exporterConfig.DisableSSL)
	cfg.WithS3ForcePathStyle(forcePath)

	svc := s3.New(sess, cfg)

	log.Infoln("Starting "+namespace+"_exporter", version.Info())
	log.Infoln("Build context", version.BuildContext())

	http.HandleFunc(*metricsPath, func(w http.ResponseWriter, r *http.Request) {
		metricsHandler(w, r, svc, exporterConfig)
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

	log.Infoln("Listening on", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}

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
		"The total number of objects for the bucket",
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
		"The total number of cds objects for the bucket",
		[]string{"bucket", "moddate", "sin"}, nil,
	)
	s3CDSSize = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "cds_objects_size_sum_bytes"),
		"The total size of all objects summed by sin and modification date",
		[]string{"bucket", "moddate", "sin"}, nil,
	)
)

// Exporter is our exporter type
type Exporter struct {
	bucket string
	svc    s3iface.S3API
}

type Config struct {
	EndpointUrl  string `yaml:"endpoint_url"`
	AwsAccessKey string `yaml:"access_key"`
	AwsSecretKey string `yaml:"secret_key"`
	DisableSSL   bool   `yaml:"disable_ssl"`
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
	var lastModified time.Time
	var numberOfObjects float64
	var totalSize int64
	var biggestObjectSize int64
	var lastObjectSize int64
	type S3Object struct {
		ObjectSin  string
		ModifyDate string
	}
	objSize := make(map[S3Object]int64)
	objCount := make(map[S3Object]uint16)

	query := &s3.ListObjectsV2Input{
		Bucket: &e.bucket,
	}

	// Continue making requests until we've listed and compared the date of every object
	startList := time.Now()
	for {
		resp, err := e.svc.ListObjectsV2(query)
		if err != nil {
			log.Errorln(err)
			ch <- prometheus.MustNewConstMetric(
				s3ListSuccess, prometheus.GaugeValue, 0, e.bucket,
			)
			return
		}
		for _, item := range resp.Contents {
			objectName := strings.ToLower(*item.Key)
			if !(strings.Contains(objectName, "/")) || !(strings.HasSuffix(objectName, ".cds")) {
				log.Debugf("Object '%v' not match. Skip.", objectName)
				continue
			}
			t := *item.LastModified
			modDate := t.Format("2006.01.02")
			re := regexp.MustCompile(`^[a-z0-9/]+_`)
			sin := re.ReplaceAllString(objectName, ``)
			re = regexp.MustCompile(`_.+\.cds$`)
			sin = re.ReplaceAllString(sin, ``)
			matched, err := regexp.MatchString(`^\d{3,5}$`, sin)
			if matched != true {
				log.Debugf("SIN '%v' in object: '%v' not match. Skip.", sin, objectName)
				continue
			}
			if err != nil {
				log.Errorln(err)
			}
			objSize[S3Object{sin, modDate}] += *item.Size
			objCount[S3Object{sin, modDate}] += 1
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

	for key, value := range objSize {
		log.Debugf("SIN: %v, Date: %v, Value: %v\n", key.ObjectSin, key.ModifyDate, value)
		ch <- prometheus.MustNewConstMetric(
			s3CDSSize, prometheus.GaugeValue, float64(value), e.bucket, key.ModifyDate, key.ObjectSin,
		)
	}

	for key, value := range objCount {
		ch <- prometheus.MustNewConstMetric(
			s3CDSObjectTotal, prometheus.GaugeValue, float64(value), e.bucket, key.ModifyDate, key.ObjectSin,
		)
	}

	listDuration := time.Now().Sub(startList).Seconds()

	ch <- prometheus.MustNewConstMetric(
		s3ListSuccess, prometheus.GaugeValue, 1, e.bucket,
	)
	ch <- prometheus.MustNewConstMetric(
		s3ListDuration, prometheus.GaugeValue, listDuration, e.bucket,
	)
	ch <- prometheus.MustNewConstMetric(
		s3LastModifiedObjectDate, prometheus.GaugeValue, float64(lastModified.UnixNano()/1e9), e.bucket,
	)
	ch <- prometheus.MustNewConstMetric(
		s3LastModifiedObjectSize, prometheus.GaugeValue, float64(lastObjectSize), e.bucket,
	)
	ch <- prometheus.MustNewConstMetric(
		s3ObjectTotal, prometheus.GaugeValue, numberOfObjects, e.bucket,
	)
	ch <- prometheus.MustNewConstMetric(
		s3BiggestSize, prometheus.GaugeValue, float64(biggestObjectSize), e.bucket,
	)
	ch <- prometheus.MustNewConstMetric(
		s3SumSize, prometheus.GaugeValue, float64(totalSize), e.bucket,
	)
}

func probeHandler(w http.ResponseWriter, r *http.Request, svc s3iface.S3API) {
	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		http.Error(w, "bucket parameter is missing", http.StatusBadRequest)
		return
	}

	exporter := &Exporter{
		bucket: bucket,
		svc:    svc,
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
	}
	defer f.Close()

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(cfg)
	if err != nil {
		log.Errorln(err)
	}
}

func main() {
	var (
		app           = kingpin.New(namespace+"_exporter", "Export metrics for CDS S3-compatible storage").DefaultEnvars()
		listenAddress = app.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9340").String()
		metricsPath   = app.Flag("web.metrics-path", "Path under which to expose metrics").Default("/metrics").String()
		inspectPath   = app.Flag("web.inspect-path", "Path under which to expose the inspect endpoint").Default("/inspect").String()
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

	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc(*inspectPath, func(w http.ResponseWriter, r *http.Request) {
		probeHandler(w, r, svc)
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
						 <head><title>AWS CDS S3 Exporter</title></head>
						 <body>
						 <h1>AWS CDS S3 Exporter</h1>
						 <p><a href="` + *inspectPath + `?bucket=BUCKET">Query metrics for objects in BUCKET</a></p>
						 <p><a href='` + *metricsPath + `'>Metrics</a></p>
						 </body>
						 </html>`))
	})

	log.Infoln("Listening on", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}

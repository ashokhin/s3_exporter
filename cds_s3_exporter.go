package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
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
		[]string{"bucket", "prefix"}, nil,
	)
	s3ListDuration = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "list_duration_seconds"),
		"The total duration of the list operation",
		[]string{"bucket", "prefix"}, nil,
	)
	s3LastModifiedObjectDate = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "last_modified_object_date"),
		"The last modified date of the object that was modified most recently",
		[]string{"bucket", "prefix"}, nil,
	)
	s3LastModifiedObjectSize = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "last_modified_object_size_bytes"),
		"The size of the object that was modified most recently",
		[]string{"bucket", "prefix"}, nil,
	)
	s3ObjectTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "objects"),
		"The total number of objects for the bucket/prefix combination",
		[]string{"bucket", "prefix"}, nil,
	)
	s3SumSize = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "objects_size_sum_bytes"),
		"The total size of all objects summed",
		[]string{"bucket", "prefix"}, nil,
	)
	s3BiggestSize = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "biggest_object_size_bytes"),
		"The size of the biggest object",
		[]string{"bucket", "prefix"}, nil,
	)
)

// Exporter is our exporter type
type Exporter struct {
	bucket string
	prefix string
	svc    s3iface.S3API
}

type Config struct {
	EndpointUrl  string `yaml:"endpoint_url"`
	AwsAccessKey string `yaml:"access_key"`
	AwsSecretKey string `yaml:"secret_key"`
	UseSSL       bool   `yaml:"use_ssl"`
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
}

// Collect metrics
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	var lastModified time.Time
	var numberOfObjects float64
	var totalSize int64
	var biggestObjectSize int64
	var lastObjectSize int64

	query := &s3.ListObjectsV2Input{
		Bucket: &e.bucket,
		Prefix: &e.prefix,
	}

	// Continue making requests until we've listed and compared the date of every object
	startList := time.Now()
	for {
		resp, err := e.svc.ListObjectsV2(query)
		if err != nil {
			log.Errorln(err)
			ch <- prometheus.MustNewConstMetric(
				s3ListSuccess, prometheus.GaugeValue, 0, e.bucket, e.prefix,
			)
			return
		}
		for _, item := range resp.Contents {
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
	listDuration := time.Now().Sub(startList).Seconds()

	ch <- prometheus.MustNewConstMetric(
		s3ListSuccess, prometheus.GaugeValue, 1, e.bucket, e.prefix,
	)
	ch <- prometheus.MustNewConstMetric(
		s3ListDuration, prometheus.GaugeValue, listDuration, e.bucket, e.prefix,
	)
	ch <- prometheus.MustNewConstMetric(
		s3LastModifiedObjectDate, prometheus.GaugeValue, float64(lastModified.UnixNano()/1e9), e.bucket, e.prefix,
	)
	ch <- prometheus.MustNewConstMetric(
		s3LastModifiedObjectSize, prometheus.GaugeValue, float64(lastObjectSize), e.bucket, e.prefix,
	)
	ch <- prometheus.MustNewConstMetric(
		s3ObjectTotal, prometheus.GaugeValue, numberOfObjects, e.bucket, e.prefix,
	)
	ch <- prometheus.MustNewConstMetric(
		s3BiggestSize, prometheus.GaugeValue, float64(biggestObjectSize), e.bucket, e.prefix,
	)
	ch <- prometheus.MustNewConstMetric(
		s3SumSize, prometheus.GaugeValue, float64(totalSize), e.bucket, e.prefix,
	)
}

func probeHandler(w http.ResponseWriter, r *http.Request, svc s3iface.S3API) {
	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		http.Error(w, "bucket parameter is missing", http.StatusBadRequest)
		return
	}

	prefix := r.URL.Query().Get("prefix")

	exporter := &Exporter{
		bucket: bucket,
		prefix: prefix,
		svc:    svc,
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(exporter)

	// Serve
	h := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	h.ServeHTTP(w, r)
}

type discoveryTarget struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

func discoveryHandler(w http.ResponseWriter, r *http.Request, svc s3iface.S3API) {
	result, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		log.Errorln(err)
		http.Error(w, "error listing buckets", http.StatusInternalServerError)
		return
	}

	targets := []discoveryTarget{}
	for _, b := range result.Buckets {
		name := aws.StringValue(b.Name)
		if name != "" {
			t := discoveryTarget{
				Targets: []string{r.Host},
				Labels: map[string]string{
					"__param_bucket": name,
				},
			}
			targets = append(targets, t)
		}
	}

	data, err := json.Marshal(targets)
	if err != nil {
		http.Error(w, "error marshalling json", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func init() {
	prometheus.MustRegister(version.NewCollector(namespace + "_exporter"))
}

func processError(err error) {
	fmt.Println(err)
	os.Exit(2)
}

func readFile(confFile string, cfg *Config) {
	f, err := os.Open(confFile)
	if err != nil {
		processError(err)
	}
	defer f.Close()

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(cfg)
	if err != nil {
		processError(err)
	}

}

func main() {
	var (
		app           = kingpin.New(namespace+"_exporter", "Export metrics for CDS S3-compatible storage").DefaultEnvars()
		listenAddress = app.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9340").String()
		metricsPath   = app.Flag("web.metrics-path", "Path under which to expose metrics").Default("/metrics").String()
		probePath     = app.Flag("web.probe-path", "Path under which to expose the probe endpoint").Default("/probe").String()
		discoveryPath = app.Flag("web.discovery-path", "Path under which to expose service discovery").Default("/discovery").String()
		expConfigPath = app.Flag("exporter-config-file", "Path to exporter config file").Default("./config.yml").String()
	)

	log.AddFlags(app)
	app.Version(version.Print(namespace + "_exporter"))
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	var sess *session.Session
	var err error
	var forcePath bool
	var exporterConfig Config

	awsCreds := credentials.NewStaticCredentials(exporterConfig.AwsAccessKey, exporterConfig.AwsSecretKey, "")
	readFile(*expConfigPath, &exporterConfig)
	log.Infof("%v", &exporterConfig)

	sess, err = session.NewSession()
	if err != nil {
		log.Errorln("Error creating sessions ", err)
	}

	cfg := aws.NewConfig()

	cfg.WithCredentials(awsCreds)
	cfg.WithEndpoint(exporterConfig.EndpointUrl)
	cfg.WithDisableSSL(exporterConfig.UseSSL)
	cfg.WithS3ForcePathStyle(forcePath)

	svc := s3.New(sess, cfg)

	log.Infoln("Starting "+namespace+"_exporter", version.Info())
	log.Infoln("Build context", version.BuildContext())

	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc(*probePath, func(w http.ResponseWriter, r *http.Request) {
		probeHandler(w, r, svc)
	})
	http.HandleFunc(*discoveryPath, func(w http.ResponseWriter, r *http.Request) {
		discoveryHandler(w, r, svc)
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
						 <head><title>AWS CDS S3 Exporter</title></head>
						 <body>
						 <h1>AWS CDS S3 Exporter</h1>
						 <p><a href="` + *probePath + `?bucket=BUCKET&prefix=PREFIX">Query metrics for objects in BUCKET that match PREFIX</a></p>
						 <p><a href='` + *metricsPath + `'>Metrics</a></p>
						 <p><a href='` + *discoveryPath + `'>Service Discovery</a></p>
						 </body>
						 </html>`))
	})

	log.Infoln("Listening on", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cavaliergopher/grab/v3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"
)

type DownloadTask struct {
	manifest   DownloadManifest
	order      Order
	httpClient *http.Client
	grabber    *grab.Client
	params     struct {
		apiKey          string
		apiEndpoint     string
		orderID         uint
		fileName        string
		concurrency     uint
		outputDir       string
		isLocalEndpoint bool
	}
}

const manifestFileName = ".ss-archive-manifest.json"
const archiveZipFileTimeFormat = "20060102-150405"

type DownloadManifest struct {
	Lock  *sync.Mutex           `json:"-"`
	Files map[string]FileStatus `json:"files"`
}

type FileStatus struct {
	FileName   string `json:"FileName"`
	Downloaded bool   `json:"Downloaded"`
	Error      string `json:"Error"`
}

type Order struct {
	DownloadToken   string    `json:"DownloadToken"`
	ArchiveDataTo   time.Time `json:"ArchiveDataTo"`
	ArchiveDataFrom time.Time `json:"ArchiveDataFrom"`
}

type fileProgress struct {
	TotalBytes int64
	Downloaded int64
	Percent    float64
	Speed      float64 // kB/s
	BytesDelta int64   // (vytes per second)
}

func NewDownloadTask() *DownloadTask {
	return &DownloadTask{
		httpClient: &http.Client{}, // no timesout because of downlaoding files
		grabber:    grab.NewClient(),
	}
}

func (o *DownloadTask) SetupParameters(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.params.apiKey, "key", "k", "", "Your API key")
	cmd.Flags().UintVarP(&o.params.orderID, "order-id", "r", 0, "the order id for all the files you want to download")
	cmd.Flags().StringVarP(&o.params.fileName, "file-name", "n", "", "an individial archive file to download")
	cmd.Flags().StringVarP(&o.params.outputDir, "output-dir", "o", "out", "output directory")
	cmd.Flags().UintVarP(&o.params.concurrency, "concurrency", "c", 1, "How many files to download concurrently. Tweak this depending on your network speed. Limit is currently 10")
	cmd.Flags().BoolVarP(&o.params.isLocalEndpoint, "isLocal", "l", false, "(used for internal testing)")
}

func (o *DownloadTask) GetMeta() Meta {
	return Meta{
		Name: "DownloadTask",
		Use:  "download",
	}
}

func (o *DownloadTask) Execute(ctx context.Context) error {
	if err := o.validateParams(); err != nil {
		return err
	}
	// // load manifest
	logrus.Infof("loading manifest...")
	err := o.getManifest(ctx)
	if err != nil {
		return err
	}

	// get order by orderID
	logrus.Infof("getting order %d ...", o.params.orderID)
	err = o.getOrder(ctx, o.params.orderID)
	if err != nil {
		return err
	}

	// get list of files to download
	logrus.Infof("generating archive file list for download...")
	files := generateListOfArchiveFiles(o.order.ArchiveDataFrom, o.order.ArchiveDataTo)
	logrus.Infof("downloading total of %d files...", len(files))

	// add one for ui thread
	concurrency := semaphore.NewWeighted(int64(o.params.concurrency))

	finishReporting := make(chan struct{})
	totalBytesDownloaded := uint64(0)
	startedAt := time.Now()
	go func() {
		// todo: substitute this rough approximation with real values but need to download all filesizes first
		const avgArchiveFilesize = 79009717 // 79 mb
		totalFiles := len(files)
		totalBytesToDownload := totalFiles * avgArchiveFilesize
		for {
			select {
			case <-finishReporting:
				return
			default:
			}
			time.Sleep(time.Second)
			progress := (float64(totalBytesDownloaded) / float64(totalBytesToDownload)) * 100
			since := time.Since(startedAt)
			eta := time.Duration((float64(since) / progress) * (100 - progress))
			logrus.Infof("Total Progress... %.2f%% complete. ETA: %s", progress, eta)

			// logrus.Infof("downloading %s: %.2f%% speed: %.2f KB/s", file, progress.Percent, progress.Speed)
		}
	}()

	// download files
	var cmdErr error
	for i, file := range files {
		// if in manifest and complete then skip
		o.manifest.Lock.Lock()
		if status, ok := o.manifest.Files[file]; ok && status.Downloaded {
			logrus.Infof("file found in manifest and already downloaded: %s", file)
			o.manifest.Lock.Unlock()
			continue
		}
		o.manifest.Lock.Unlock()

		concurrency.Acquire(ctx, 1)
		go func() {
			defer concurrency.Release(1)

			logrus.Infof("downloading %d of %d files...", i+1, len(files))
			err = o.downloadFile(ctx, file, func(progress fileProgress) {
				atomic.AddUint64(&totalBytesDownloaded, uint64(progress.Downloaded))
				logrus.Infof("downloading %s: %.2f%% speed: %.2f KB/s", file, progress.Percent, progress.Speed)
			})
			if err != nil {
				logrus.Errorf("error downloading file %s: %s", file, err)
				o.manifest.Lock.Lock()
				o.manifest.Files[file] = FileStatus{
					FileName:   file,
					Downloaded: false,
					Error:      err.Error(),
				}
				o.manifest.Lock.Unlock()
				err = o.updateManifest(ctx)
				if err != nil {
					logrus.Errorf("error updating manifest: %s", err)
				}
				cmdErr = err // propagate to fail at the end
				return
			}
			o.manifest.Lock.Lock()
			o.manifest.Files[file] = FileStatus{
				FileName:   file,
				Downloaded: true,
				Error:      "",
			}
			o.manifest.Lock.Unlock()
			err = o.updateManifest(ctx)

		}()
	}

	// wait for all routines to release
	concurrency.Acquire(ctx, int64(o.params.concurrency))
	finishReporting <- struct{}{}

	if cmdErr != nil {
		logrus.Error("Completed with error. Please run again to retry failed files.")
		return cmdErr
	}

	logrus.Infof("Completed. Downloaded %d files", len(files))
	return nil
}

func (o *DownloadTask) getOrder(ctx context.Context, orderID uint) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, o.params.apiEndpoint+"/order/"+strconv.Itoa(int(orderID)), nil)
	if err != nil {
		return nil
	}
	req.Header.Add("X-API-KEY", o.params.apiKey)

	resp, err := o.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return json.NewDecoder(resp.Body).Decode(&o.order)
}

func (o *DownloadTask) downloadFile(ctx context.Context, fileName string, reportProgress func(fileProgress)) error {

	fullfilename := fmt.Sprintf(o.params.apiEndpoint+"/archive/download/%s?token=%s", fileName, o.order.DownloadToken)
	req, err := grab.NewRequest(o.params.outputDir+"/"+fileName+".zip", fullfilename)
	if err != nil {
		return err
	}

	resp := o.grabber.Do(req)
	if resp == nil {
		return fmt.Errorf("failed to start download")
	}
	if resp.HTTPResponse.StatusCode != http.StatusOK {
		if resp.HTTPResponse.StatusCode == http.StatusPaymentRequired {
			return fmt.Errorf("payment required or order expired")
		}
		return fmt.Errorf("unexpected status code: %d", resp.HTTPResponse.StatusCode)
	}

Loop:
	for {
		select {
		case <-resp.Done:
			break Loop
		default:
		}
		time.Sleep(time.Second)
		reportProgress(fileProgress{
			TotalBytes: (resp.Size()),
			Downloaded: (resp.BytesComplete()),
			Percent:    100 * resp.Progress(),
			Speed:      resp.BytesPerSecond() / 1024,
			BytesDelta: int64(resp.BytesPerSecond()),
		})
	}

	if err := resp.Err(); err != nil {
		return err
	}

	logrus.Infof("downloaded successfully %s", fileName)

	return nil
}

func generateListOfArchiveFiles(from, to time.Time) []string {
	var files []string
	for t := from; t.Before(to); t = t.Add(time.Hour) {
		files = append(files, t.Format(archiveZipFileTimeFormat))
	}
	return files
}

func (o *DownloadTask) getManifest(ctx context.Context) error {
	// / set defaults
	o.manifest.Lock = &sync.Mutex{}
	o.manifest.Files = make(map[string]FileStatus)

	// if file exists
	err := os.MkdirAll(o.params.outputDir, 0755)
	if err != nil {
		return errors.Wrap(err, "failed to create output directory")
	}
	_, err = os.Stat(o.params.outputDir + "/" + manifestFileName)
	if err != nil {
		// will be created if it doesn't exist on update
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	f, err := os.OpenFile(o.params.outputDir+"/"+manifestFileName, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	return json.NewDecoder(f).Decode(&o.manifest)
}

func (o *DownloadTask) updateManifest(ctx context.Context) error {
	f, err := os.OpenFile(o.params.outputDir+"/"+manifestFileName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	err = json.NewEncoder(f).Encode(o.manifest)
	if err != nil {
		return err
	}
	return nil
}

func (o *DownloadTask) validateParams() error {
	if o.params.apiKey == "" {
		return errors.New("missing API key")
	}
	if o.params.orderID == 0 && o.params.fileName == "" {
		return errors.New("missing order ID or file name")
	}
	if o.params.outputDir == "" {
		o.params.outputDir = "."
	}
	o.params.apiEndpoint = "https://api.solanastreaming.com"
	if o.params.isLocalEndpoint {
		o.params.apiEndpoint = "http://localhost:8000"
	}
	if o.params.concurrency == 0 {
		o.params.concurrency = 1
	}
	if o.params.concurrency > 10 {
		return errors.New("concurrency limit is 10")
	}
	return nil
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
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
	Speed      float64 // mB/s
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
	// cmd.Flags().StringVarP(&o.params.fileName, "file-name", "n", "", "an individial archive file to download")
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
	os.MkdirAll(o.params.outputDir, 0755)
	// // load manifest
	currentFiles, err := o.getCurrentFiles(ctx)
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

	// remove already downloaded files
	filesToDownload := []string{}
	for _, file := range files {
		if inSlice(currentFiles, file) {
			continue
		}
		filesToDownload = append(filesToDownload, file)
	}
	if len(filesToDownload) == 0 {
		logrus.Infof("all files already downloaded")
		return nil
	}
	logrus.Infof("downloading total of %d files...", len(filesToDownload))

	// get filesizes so we can calculate progress
	totalBytesToDownload, err := o.getMetadata(ctx, filesToDownload)
	if err != nil {
		return err
	}

	// add one for ui thread
	concurrency := semaphore.NewWeighted(int64(o.params.concurrency))

	individualProgress := []fileProgress{}
	finishReporting := make(chan struct{})
	startedAt := time.Now()
	go func() {
		// todo: substitute this rough approximation with real values but need to download all filesizes first
		for {
			select {
			case <-finishReporting:
				return
			default:
			}
			time.Sleep(time.Second)
			totalBytesDownloaded := int64(0)
			speed := float64(0)
			for _, v := range individualProgress {
				totalBytesDownloaded += v.Downloaded
				speed += v.Speed
			}

			progress := (float64(totalBytesDownloaded) / float64(totalBytesToDownload)) * 100
			since := time.Since(startedAt)
			eta := time.Duration((float64(since) / progress) * (100 - progress))
			fmt.Printf("\rTotal Progress... %.2f%% complete. Current Speed: %.2f MB/s (%.2fMB/%.2fMB) ETA: %s", progress, speed, float64(totalBytesDownloaded)/1000000, float64(totalBytesToDownload)/1000000, eta)

			// logrus.Infof("downloading %s: %.2f%% speed: %.2f KB/s", file, progress.Percent, progress.Speed)
		}
	}()

	// download files
	var cmdErr error
	for i, file := range filesToDownload {
		concurrency.Acquire(ctx, 1)
		individualProgress = append(individualProgress, fileProgress{})
		go func() {
			defer concurrency.Release(1)

			logrus.Debugf("downloading %d of %d files...", i+1, len(filesToDownload))
			err = o.downloadFile(ctx, file, func(progress fileProgress) {
				individualProgress[i] = progress
				// logrus.Infof("downloading %s: %.2f%% speed: %.2f KB/s", file, progress.Percent, progress.Speed)
			})
			if err != nil {
				logrus.Errorf("error downloading file %s: %s", file, err)
				cmdErr = err // propagate to fail at the end
				return
			}

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

func (o *DownloadTask) getMetadata(ctx context.Context, files []string) (uint, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	request := map[string]interface{}{
		"files": files,
	}
	body, err := json.Marshal(request)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.params.apiEndpoint+"/archive/metadata", bytes.NewBuffer(body))
	if err != nil {
		return 0, err
	}
	req.Header.Add("X-API-KEY", o.params.apiKey)

	resp, err := o.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	response := []struct {
		Swaps    uint `db:"swaps"`
		NewPairs uint `db:"newpairs"`
		Filesize uint `db:"filesize"`
	}{}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return 0, err
	}

	total := uint(0)
	for _, v := range response {
		total += v.Filesize
	}

	return total, nil
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
			Speed:      resp.BytesPerSecond() / 1000000,
			BytesDelta: int64(resp.BytesPerSecond()),
		})
	}

	if err := resp.Err(); err != nil {
		return err
	}

	logrus.Debugf("downloaded successfully %s", fileName)

	return nil
}

func inSlice(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func generateListOfArchiveFiles(from, to time.Time) []string {
	var files []string
	for t := from; t.Before(to); t = t.Add(time.Hour) {
		files = append(files, t.Format(archiveZipFileTimeFormat))
	}
	return files
}

func (o *DownloadTask) getCurrentFiles(ctx context.Context) ([]string, error) {
	files, err := os.ReadDir(o.params.outputDir)
	if err != nil {
		return nil, err
	}
	alreadyDownloaded := []string{}
	for _, v := range files {
		if v.IsDir() {
			continue
		}
		// check extension
		if len(v.Name()) < 4 || v.Name()[len(v.Name())-4:] != ".zip" {
			continue
		}
		alreadyDownloaded = append(alreadyDownloaded, v.Name()[0:len(v.Name())-4])
	}
	return alreadyDownloaded, nil
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

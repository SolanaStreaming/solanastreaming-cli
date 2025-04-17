package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"

	"github.com/gagliardetto/solana-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"
)

type ReduceTask struct {
	amms           []solana.PublicKey
	baseTokenMints []solana.PublicKey
	wallets        []solana.PublicKey
	params         struct {
		amms           string
		baseTokenMints string
		wallets        string
		paramsFile     string
		dataInDir      string
		dataOutDir     string
		concurrency    int
	}
}

func NewReduceTask() *ReduceTask {
	return &ReduceTask{}
}

func (o *ReduceTask) SetupParameters(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.params.amms, "amm", "a", "", "Include any events with these AMMs. (Comma separated list)")
	cmd.Flags().StringVarP(&o.params.baseTokenMints, "baseTokenMint", "b", "", "Include any events with these mints. (Comma separated list)")
	cmd.Flags().StringVarP(&o.params.wallets, "wallet", "w", "", "Include any events with this wallets. (Comma separated list)")
	cmd.Flags().StringVarP(&o.params.paramsFile, "params-file", "f", "", "JSON file with input params. See docs for format. Supply as many addresses as you want.")
	cmd.Flags().StringVarP(&o.params.dataInDir, "in-data-dir", "i", "out", "The dir to get the data from for streaming")
	cmd.Flags().StringVarP(&o.params.dataOutDir, "out-data-dir", "o", "out-reduced", "The dir to get the data from for streaming")
	cmd.Flags().IntVarP(&o.params.concurrency, "concurrency", "c", 10, "How many files to process at once. Adjust this depending on your CPU and memory. Default is 10.")
}

func (o *ReduceTask) GetMeta() Meta {
	return Meta{
		Name:        "ReduceTask",
		Use:         "reduce",
		Description: "Reduce local archive files by filtering the data to only the events you are interested in. This will create a copy archive file(s) with the filtered data.",
	}
}

type EventRow struct {
	Slot uint64 `json:"slot"`
	Sig  string `json:"signature"`
	Pair *struct {
		AmmAccount string `json:"ammAccount"`
		BaseToken  struct {
			Account string `json:"account"`
		}
	} `json:"pair"`
	Swap *struct {
		AmmAccount    string `json:"ammAccount"`
		BaseTokenMint string `json:"baseTokenMint"`
		WalletAccount string `json:"walletAccount"`
	} `json:"swap"`
}

func (o *ReduceTask) Execute(ctx context.Context) error {
	err := o.processParams()
	if err != nil {
		return err
	}

	inFiles, err := o.getDataFiles()
	if err != nil {
		return err
	}

	filterFunc, err := o.makeFilterFunc()
	if err != nil {
		return err
	}

	sem := semaphore.NewWeighted(int64(o.params.concurrency))
	errs := []error{}
	for _, v := range inFiles {
		err := sem.Acquire(ctx, 1)
		if err != nil {
			return err
		}
		go func(fileName string) {
			defer sem.Release(1)
			err := o.processFile(fileName, filterFunc)
			if err != nil {
				errs = append(errs, err)
			}
		}(v)
	}
	// wait for all goroutines to finish
	if err := sem.Acquire(ctx, int64(o.params.concurrency)); err != nil {
		return err
	}

	// check for errors
	if len(errs) > 0 {
		for _, err := range errs {
			logrus.Errorf("Error processing file: %s", err.Error())
		}
		return errors.New("errors occurred during processing")
	}

	logrus.Infof("Reduced and copied %d files to %s", len(inFiles), o.params.dataOutDir)

	return nil
}

func (o *ReduceTask) getDataFiles() ([]string, error) {
	// loop through dir contents
	files, err := os.ReadDir(o.params.dataInDir)
	if err != nil {
		return nil, err
	}
	filtered := []string{}
	for _, v := range files {
		if v.IsDir() {
			continue
		}
		// check extension
		if len(v.Name()) < 4 || v.Name()[len(v.Name())-4:] != ".zip" {
			continue
		}
		filtered = append(filtered, v.Name())
	}
	// ensure they are ordered by date (oldest first)
	return filtered, nil
}

func (o *ReduceTask) processFile(fileName string, filterFunc func(EventRow) bool) error {
	logrus.Infof("Processing file %s", fileName)
	r, err := zip.OpenReader(o.params.dataInDir + "/" + fileName)
	if err != nil {
		return err
	}
	unzippedFiles := []string{}

	// Iterate through the files in the archive and unzip them
	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		outFile, err := os.OpenFile(o.params.dataInDir+"/"+f.Name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return err
		}
		_, err = io.Copy(outFile, rc)
		if err != nil {
			return err
		}
		rc.Close()
		outFile.Close()
		unzippedFiles = append(unzippedFiles, f.Name)
	}
	r.Close()

	// ensure outdir exists no err
	os.MkdirAll(o.params.dataOutDir, 0755)

	// unzipped all files, and filter rows into new file
	filteredFiles := []string{}
	for _, v := range unzippedFiles {
		inFile, err := os.Open(o.params.dataInDir + "/" + v)
		if err != nil {
			return err
		}

		filteredFile := v
		outFile, err := os.OpenFile(o.params.dataOutDir+"/"+filteredFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return err
		}
		filteredFiles = append(filteredFiles, filteredFile)

		// foreach line in old file
		scanner := bufio.NewScanner(inFile)
		for scanner.Scan() {
			row := scanner.Bytes()
			eventRow := EventRow{}
			err := json.Unmarshal(row, &eventRow)
			if err != nil {
				return errors.Wrap(err, "cant unmarshal event")
			}
			// include in new file
			if filterFunc(eventRow) {
				io.Copy(outFile, bytes.NewReader(append(row, '\n')))
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		inFile.Close()
		outFile.Close()
	}

	// now compress new archive
	f, err := os.OpenFile(o.params.dataOutDir+"/"+fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	w := zip.NewWriter(f)

	for _, v := range filteredFiles {
		aw, err := w.Create(v)
		if err != nil {
			return err
		}

		fr, err := os.Open(o.params.dataOutDir + "/" + v)
		if err != nil {
			return err
		}
		io.Copy(aw, fr)
		fr.Close()
	}
	err = w.Close()
	if err != nil {
		return err
	}
	f.Close()

	// remove all intermediate files
	for _, v := range unzippedFiles {
		err := os.Remove(o.params.dataInDir + "/" + v)
		if err != nil {
			return err
		}
	}
	for _, v := range filteredFiles {
		err := os.Remove(o.params.dataOutDir + "/" + v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *ReduceTask) makeFilterFunc() (func(EventRow) bool, error) {
	// make filter function
	filterFunc := func(row EventRow) bool {
		var amm, wallet, baseTokenMint solana.PublicKey
		var err error
		if row.Pair != nil {
			amm, err = solana.PublicKeyFromBase58(row.Pair.AmmAccount)
			if err != nil {
				logrus.Error(errors.Wrapf(err, "Error parsing AMM account (\"%s\") for pair", row.Pair.AmmAccount).Error())
			}
			baseTokenMint, err = solana.PublicKeyFromBase58(row.Pair.BaseToken.Account)
			if err != nil {
				logrus.Error(errors.Wrapf(err, "Error parsing BaseTokenAccount (\"%s\") for pair", row.Pair.BaseToken.Account).Error())
			}
		} else if row.Swap != nil {
			amm, err = solana.PublicKeyFromBase58(row.Swap.AmmAccount)
			if err != nil {
				logrus.Error(errors.Wrapf(err, "Error parsing AmmAccount (\"%s\") for swap", row.Swap.AmmAccount).Error())
			}
			baseTokenMint, err = solana.PublicKeyFromBase58(row.Swap.BaseTokenMint)
			if err != nil {
				logrus.Error(errors.Wrapf(err, "Error parsing BaseTokenMint (\"%s\") for swap", row.Swap.BaseTokenMint).Error())
			}
			wallet, err = solana.PublicKeyFromBase58(row.Swap.WalletAccount)
			if err != nil {
				logrus.Error(errors.Wrapf(err, "Error parsing WalletAccount (\"%s\") for swap", row.Swap.WalletAccount).Error())
			}
		}

		if len(o.amms) != 0 {
			// check if any of the amms match
			for _, v := range o.amms {
				if v.Equals(amm) {
					return true
				}
			}
		}
		if len(o.baseTokenMints) != 0 {
			// check if any of the baseTokenMints match
			for _, v := range o.baseTokenMints {
				if v.Equals(baseTokenMint) {
					return true
				}
			}
		}
		if len(o.wallets) != 0 {
			// check if any of the wallets match
			for _, v := range o.wallets {
				if v.Equals(wallet) {
					return true
				}
			}
		}

		return false
	}
	return filterFunc, nil
}

func (o *ReduceTask) processParams() error {
	//amms
	for _, v := range strings.Split(o.params.amms, ",") {
		if v == "" {
			continue
		}
		o.amms = append(o.amms, solana.MustPublicKeyFromBase58(v))
	}
	// basetoken mints
	for _, v := range strings.Split(o.params.baseTokenMints, ",") {
		if v == "" {
			continue
		}
		o.baseTokenMints = append(o.baseTokenMints, solana.MustPublicKeyFromBase58(v))
	}

	// wallets
	for _, v := range strings.Split(o.params.wallets, ",") {
		if v == "" {
			continue
		}
		o.wallets = append(o.wallets, solana.MustPublicKeyFromBase58(v))
	}

	return nil
}

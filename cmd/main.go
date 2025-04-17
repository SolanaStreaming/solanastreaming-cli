package main

import (
	"context"
	"errors"
	"log"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func main() {

	logrus.SetLevel(logrus.DebugLevel)
	tm := NewTaskManager()
	tasks := []Task{
		NewDownloadTask(),
		NewSimulateTask(),
		NewReduceTask(),
	}
	rootCmd := &cobra.Command{
		Use:   "ss-cli",
		Short: "run solanastreaming commands",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("please select command")
		},
	}
	for _, v := range tasks {
		rootCmd.AddCommand(tm.GetCommand(v))
	}

	err := rootCmd.ExecuteContext(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

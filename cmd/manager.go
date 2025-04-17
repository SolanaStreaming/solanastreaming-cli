package main

import (
	"context"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type TaskManager struct {
}

func NewTaskManager() TaskManager {
	return TaskManager{}
}

func (o *TaskManager) GetCommand(tsk Task) *cobra.Command {
	cmd := &cobra.Command{}
	meta := tsk.GetMeta()
	cmd.Use = meta.Use
	cmd.Short = meta.Description
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return o.ExecuteTask(cmd.Context(), tsk)
	}
	tsk.SetupParameters(cmd)
	return cmd
}

func (o *TaskManager) ExecuteTask(ctx context.Context, tsk Task) error {
	meta := tsk.GetMeta()
	log.Infof("Running: " + meta.Name)
	return tsk.Execute(ctx)
}

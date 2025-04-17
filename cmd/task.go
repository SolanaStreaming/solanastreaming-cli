package main

import (
	"context"
	"errors"

	"github.com/spf13/cobra"
)

type Task interface {
	// SetupParameters used to register task cli flags and inputs
	SetupParameters(cmd *cobra.Command)

	// GetMeta returns Meta which contains task name and description
	GetMeta() Meta

	// Execute runs the task logic
	Execute(ctx context.Context) error
}

var ErrNoOp = errors.New("please select a subcommand")

type Meta struct {
	Name        string
	Use         string
	Description string
}

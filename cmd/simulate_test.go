package main

import (
	"context"
	"testing"

	"github.com/test-go/testify/assert"
)

func TestSimulateTask(t *testing.T) {
	st := NewSimulateTask()
	st.params.dataDir = "../out"
	err := st.RunSimulation(context.Background(), 1)
	assert.Nil(t, err)
}

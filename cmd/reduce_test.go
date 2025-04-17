package main

import (
	"context"
	"testing"
)

func TestReduceTask(t *testing.T) {
	task := NewReduceTask()
	task.params.dataInDir = "../out"
	task.params.dataOutDir = "../out-reduced"
	task.params.concurrency = 10
	task.params.baseTokenMints = "F58xDnQ5JGCLmRM7vg5EfGrow4LuLv8M1e9UCGb8pump"
	task.Execute(context.Background())
}

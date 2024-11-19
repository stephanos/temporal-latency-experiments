package updateandstart

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	. "github.com/dandavison/temporal-latency-experiments/must"
	"github.com/dandavison/temporal-latency-experiments/tle"
	"github.com/pborman/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

const (
	UpdateName = "my-update"
)

func Run(c client.Client, l sdklog.Logger, iterations int) tle.Results {
	defer func() {
		var err = recover()
		assert.Always(err == nil, "[WKL] Update benchmark succeeded: update-and-start", map[string]any{"err": err})
	}()
	ctx := context.Background()

	latencies := []int64{}
	wfts := []int{}
	for i := 0; i < iterations; i++ {
		workflowID := "update-and-start-" + uuid.New()
		fmt.Fprintf(os.Stderr, workflowID+"\n")

		start := time.Now()

		Must(c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID:                       workflowID,
			TaskQueue:                tle.TaskQueue,
			WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		}, MyWorkflow))

		u := Must(c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
			WorkflowID:   workflowID,
			UpdateName:   UpdateName,
			UpdateID:     strconv.Itoa(i),
			WaitForStage: client.WorkflowUpdateStageCompleted,
		}))

		Must1(u.Get(ctx, nil))

		latency := time.Since(start).Nanoseconds()
		latencies = append(latencies, latency)
		time.Sleep(100 * time.Millisecond)
	}

	return tle.Results{
		LatenciesNs: latencies,
		Wfts:        wfts,
	}
}

func MyWorkflow(ctx workflow.Context) error {
	var done bool
	err := workflow.SetUpdateHandler(
		ctx,
		UpdateName,
		func(ctx workflow.Context) error {
			done = true
			return nil
		})
	if err != nil {
		return err
	}
	workflow.AwaitWithTimeout(ctx, 60*time.Second, func() bool {
		return done
	})
	return nil
}

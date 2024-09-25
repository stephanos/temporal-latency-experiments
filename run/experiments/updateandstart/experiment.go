package updateandstart

import (
	"context"
	"strconv"
	"time"

	. "github.com/dandavison/temporal-latency-experiments/must"
	"github.com/dandavison/temporal-latency-experiments/tle"
	"github.com/pborman/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

const (
	UpdateName     = "my-update"
	DoneSignalName = "Done"
)

func Run(c client.Client, l sdklog.Logger, iterations int) tle.Results {
	ctx := context.Background()

	latencies := []int64{}
	wfts := []int{}
	for i := 0; i < iterations; i++ {
		workflowID := "update-and-start-" + uuid.New()

		policy := enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
		if i%2000 == 0 {
			policy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
		}

		start := time.Now()

		Must(c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID:                       workflowID,
			TaskQueue:                tle.TaskQueue,
			WorkflowIDConflictPolicy: policy,
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

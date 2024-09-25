package updatewithstart

import (
	"context"
	"strconv"
	"time"

	. "github.com/dandavison/temporal-latency-experiments/must"
	"github.com/dandavison/temporal-latency-experiments/tle"
	"github.com/dandavison/tle/experiments/update"
	"github.com/pborman/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
)

const (
	UpdateName = "my-update"
)

func Run(c client.Client, l sdklog.Logger, iterations int) tle.Results {
	ctx := context.Background()

	latencies := []int64{}
	wfts := []int{}
	for i := 0; i < iterations; i++ {
		workflowID := "update-with-start-" + uuid.New()

		policy := enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
		if i%2000 == 0 {
			policy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
		}

		start := time.Now()

		op := client.NewUpdateWithStartWorkflowOperation(client.UpdateWorkflowOptions{
			WorkflowID:   workflowID,
			UpdateName:   UpdateName,
			UpdateID:     strconv.Itoa(i),
			WaitForStage: client.WorkflowUpdateStageCompleted,
		})

		c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID:                       workflowID,
			TaskQueue:                tle.TaskQueue,
			WorkflowIDConflictPolicy: policy,
			WithStartOperation:       op,
		}, update.MyWorkflow)

		Must1(Must(op.Get(ctx)).Get(ctx, nil))

		latency := time.Since(start).Nanoseconds()
		latencies = append(latencies, latency)
		time.Sleep(100 * time.Millisecond)
	}

	return tle.Results{
		LatenciesNs: latencies,
		Wfts:        wfts,
	}
}

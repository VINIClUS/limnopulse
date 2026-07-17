package worker

import "context"

const (
	ConsumerJobs     = "jobs"
	ConsumerFeedback = "ses_feedback"
)

type RunConsumer interface {
	Run(context.Context) RunSummary
}

type Supervisor struct {
	Jobs     RunConsumer
	Feedback RunConsumer
}

type consumerResult struct {
	name    string
	summary RunSummary
}

func (supervisor Supervisor) Run(ctx context.Context) RunSummary {
	if supervisor.Jobs == nil || supervisor.Feedback == nil {
		return RunSummary{
			Result: "fatal_failure", ExitCode: 1, Fatal: true,
			ErrorCategories: map[string]int{"configuration": 1},
		}
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan consumerResult, 2)
	go func() { results <- consumerResult{name: ConsumerJobs, summary: supervisor.Jobs.Run(runCtx)} }()
	go func() { results <- consumerResult{name: ConsumerFeedback, summary: supervisor.Feedback.Run(runCtx)} }()

	collected := make(map[string]RunSummary, 2)
	for len(collected) < 2 {
		result := <-results
		if len(collected) == 0 && !result.summary.Fatal && ctx.Err() == nil {
			result.summary.Fatal = true
			result.summary.Graceful = false
			result.summary.Result = "fatal_failure"
			result.summary.ExitCode = 1
			if result.summary.ErrorCategories == nil {
				result.summary.ErrorCategories = map[string]int{}
			}
			result.summary.ErrorCategories["unexpected_consumer_exit"]++
		}
		collected[result.name] = result.summary
		if len(collected) == 1 || result.summary.Fatal {
			cancel()
		}
	}
	return aggregateConsumers(collected)
}

func aggregateConsumers(results map[string]RunSummary) RunSummary {
	summary := RunSummary{
		Graceful: true, ErrorCategories: map[string]int{},
		Consumers: map[string]ConsumerSummary{},
	}
	for _, name := range []string{ConsumerJobs, ConsumerFeedback} {
		result := results[name]
		summary.Consumers[name] = toConsumerSummary(result)
		summary.Fatal = summary.Fatal || result.Fatal
		summary.Graceful = summary.Graceful && result.Graceful
		summary.DrainTimedOut = summary.DrainTimedOut || result.DrainTimedOut
		summary.MessagesReceived += result.MessagesReceived
		summary.MessagesDeleted += result.MessagesDeleted
		summary.VisibilityChanged += result.VisibilityChanged
		summary.QueueErrors += result.QueueErrors
		for category, count := range result.ErrorCategories {
			summary.ErrorCategories[category] += count
		}
	}
	if summary.Fatal || !summary.Graceful {
		summary.Result = "fatal_failure"
		summary.ExitCode = 1
		summary.Graceful = false
	} else {
		summary.Result = "success"
		summary.ExitCode = 0
	}
	return summary
}

func toConsumerSummary(summary RunSummary) ConsumerSummary {
	errorsCopy := make(map[string]int, len(summary.ErrorCategories))
	for category, count := range summary.ErrorCategories {
		errorsCopy[category] = count
	}
	return ConsumerSummary{
		Graceful: summary.Graceful, Fatal: summary.Fatal, DrainTimedOut: summary.DrainTimedOut,
		MessagesReceived: summary.MessagesReceived, MessagesDeleted: summary.MessagesDeleted,
		VisibilityChanged: summary.VisibilityChanged, QueueErrors: summary.QueueErrors,
		ErrorCategories: errorsCopy,
	}
}

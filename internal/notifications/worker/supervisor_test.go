package worker

import (
	"context"
	"sync"
	"testing"
)

func TestSupervisorRunsJobsAndFeedbackIndependentlyAndCancelsSiblingOnFatal(t *testing.T) {
	feedbackStarted := make(chan struct{})
	feedbackStopped := make(chan struct{})
	jobs := runConsumerFunc(func(context.Context) RunSummary {
		return RunSummary{
			Fatal: true, Result: "fatal_failure", ExitCode: 1,
			MessagesReceived: 2, QueueErrors: 1, ErrorCategories: map[string]int{"queue_receive_failure": 1},
		}
	})
	feedback := runConsumerFunc(func(ctx context.Context) RunSummary {
		close(feedbackStarted)
		<-ctx.Done()
		close(feedbackStopped)
		return RunSummary{
			Graceful: true, Result: "success", MessagesReceived: 3, MessagesDeleted: 2,
			VisibilityChanged: 1, ErrorCategories: map[string]int{},
		}
	})

	summary := (Supervisor{Jobs: jobs, Feedback: feedback}).Run(context.Background())
	select {
	case <-feedbackStarted:
	default:
		t.Fatal("feedback consumer was not started")
	}
	select {
	case <-feedbackStopped:
	default:
		t.Fatal("feedback consumer was not supervised after jobs fatal")
	}
	if !summary.Fatal || summary.Graceful || summary.ExitCode != 1 || summary.MessagesReceived != 5 ||
		summary.MessagesDeleted != 2 || summary.VisibilityChanged != 1 || summary.QueueErrors != 1 {
		t.Fatalf("summary = %#v", summary)
	}
	if len(summary.Consumers) != 2 || !summary.Consumers[ConsumerJobs].Fatal ||
		!summary.Consumers[ConsumerFeedback].Graceful {
		t.Fatalf("consumer summaries = %#v", summary.Consumers)
	}
}

func TestSupervisorAccountsForBothConcurrentDrainResults(t *testing.T) {
	var started sync.WaitGroup
	started.Add(2)
	release := make(chan struct{})
	consumer := func(summary RunSummary) runConsumerFunc {
		return func(context.Context) RunSummary {
			started.Done()
			<-release
			return summary
		}
	}
	jobs := consumer(RunSummary{Graceful: true, MessagesReceived: 1, MessagesDeleted: 1, ErrorCategories: map[string]int{}})
	feedback := consumer(RunSummary{Graceful: true, DrainTimedOut: true, Fatal: true, MessagesReceived: 4, ErrorCategories: map[string]int{"drain_timeout": 1}})
	done := make(chan RunSummary, 1)
	go func() { done <- (Supervisor{Jobs: jobs, Feedback: feedback}).Run(context.Background()) }()
	started.Wait()
	close(release)
	summary := <-done
	if !summary.Fatal || !summary.DrainTimedOut || summary.MessagesReceived != 5 || summary.MessagesDeleted != 1 ||
		!summary.Consumers[ConsumerFeedback].DrainTimedOut {
		t.Fatalf("drain summary = %#v", summary)
	}
}

func TestSupervisorFeedbackFatalCancelsJobsConsumer(t *testing.T) {
	jobsStopped := make(chan struct{})
	jobs := runConsumerFunc(func(ctx context.Context) RunSummary {
		<-ctx.Done()
		close(jobsStopped)
		return RunSummary{Graceful: true, ErrorCategories: map[string]int{}}
	})
	feedback := runConsumerFunc(func(context.Context) RunSummary {
		return RunSummary{Fatal: true, ErrorCategories: map[string]int{"feedback_queue_receive_failure": 1}}
	})
	summary := (Supervisor{Jobs: jobs, Feedback: feedback}).Run(context.Background())
	select {
	case <-jobsStopped:
	default:
		t.Fatal("jobs consumer was not cancelled after feedback fatal")
	}
	if !summary.Fatal || !summary.Consumers[ConsumerFeedback].Fatal {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestSupervisorExternalCancellationLetsBothConsumersDrain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var started sync.WaitGroup
	started.Add(2)
	consumer := runConsumerFunc(func(ctx context.Context) RunSummary {
		started.Done()
		<-ctx.Done()
		return RunSummary{Graceful: true, ErrorCategories: map[string]int{}}
	})
	done := make(chan RunSummary, 1)
	go func() { done <- (Supervisor{Jobs: consumer, Feedback: consumer}).Run(ctx) }()
	started.Wait()
	cancel()
	summary := <-done
	if !summary.Graceful || summary.Fatal || !summary.Consumers[ConsumerJobs].Graceful ||
		!summary.Consumers[ConsumerFeedback].Graceful {
		t.Fatalf("external cancellation summary = %#v", summary)
	}
}

func TestSupervisorTreatsUnexpectedCleanConsumerExitAsFatalAndCancelsSibling(t *testing.T) {
	siblingStopped := make(chan struct{})
	jobs := runConsumerFunc(func(context.Context) RunSummary {
		return RunSummary{Graceful: true, ErrorCategories: map[string]int{}}
	})
	feedback := runConsumerFunc(func(ctx context.Context) RunSummary {
		<-ctx.Done()
		close(siblingStopped)
		return RunSummary{Graceful: true, ErrorCategories: map[string]int{}}
	})
	summary := (Supervisor{Jobs: jobs, Feedback: feedback}).Run(context.Background())
	select {
	case <-siblingStopped:
	default:
		t.Fatal("sibling consumer was left running after unexpected clean exit")
	}
	if !summary.Fatal || summary.ErrorCategories["unexpected_consumer_exit"] != 1 {
		t.Fatalf("summary = %#v", summary)
	}
}

type runConsumerFunc func(context.Context) RunSummary

func (run runConsumerFunc) Run(ctx context.Context) RunSummary { return run(ctx) }

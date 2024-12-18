package cron

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/test-go/testify/assert"
)

// Many tests schedule a job for every second, and then wait at most a second
// for it to run.  This amount is just slightly larger than 1 second to
// compensate for a few milliseconds of runtime.
const ONE_SECOND = 1*time.Second + 10*time.Millisecond

// Start and stop cron with no entries.
func TestNoEntries(t *testing.T) {
	cron := New()
	cron.Start()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-stop(cron):
	}
}

// Start, stop, then add an entry. Verify entry doesn't run.
func TestStopCausesJobsToNotRun(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.Start()
	cron.Stop()
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test1", nil)

	select {
	case <-time.After(1 * ONE_SECOND):
		// No job ran!
	case <-wait(wg):
		t.FailNow()
	}
}

// Add a job, start cron, expect it runs.
func TestAddBeforeRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test2", nil)
	cron.Start()
	defer cron.Stop()

	// Give cron 2 seconds to run our job (which is always activated).
	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Start cron, add a job, expect it runs.
func TestAddWhileRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.Start()
	defer cron.Stop()
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test3", nil)

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test timing with Entries.
func TestSnapshotEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddFunc("@every 2s", func() { wg.Done() }, "test4", nil)
	cron.Start()
	defer cron.Stop()

	// Cron should fire in 2 seconds. After 1 second, call Entries.
	time.Sleep(ONE_SECOND)

	// Even though Entries was called, the cron should fire at the 2 second mark.
	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}

}

// Test running the same job twice.
func TestRunningJobTwice(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New()
	cron.AddFunc("0 0 0 1 1 ?", func() {}, "test9", nil)
	cron.AddFunc("0 0 0 31 12 ?", func() {}, "test10", nil)
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test11", nil)

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(3 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

func TestRunningMultipleSchedules(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New()
	cron.AddFunc("0 0 0 1 1 ?", func() {}, "test12", nil)
	cron.AddFunc("0 0 0 31 12 ?", func() {}, "test13", nil)
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test14", nil)
	cron.Schedule(Every(time.Minute), FuncJob(func() {}), "test15")
	cron.Schedule(Every(time.Second), FuncJob(func() { wg.Done() }), "test16")
	cron.Schedule(Every(time.Hour), FuncJob(func() {}), "test17")

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

func TestRunEverMsWithOneMs(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(3)
	var occupied atomic.Bool

	cron := New()
	s := time.Now()
	cron.Schedule(Every(500*time.Millisecond), FuncJob(func() {
		start := time.Now()
		if occupied.Load() {
			return
		}
		occupied.Store(true)
		defer func() {
			occupied.Store(false)
		}()
		time.Sleep(100 * time.Millisecond)
		t.Logf("exec %v", time.Since(start))
		wg.Done()
	}), "test20")
	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
		t.Log(time.Since(s))
		if time.Since(s) < 1500*time.Millisecond {
			t.Errorf("time %v", time.Since(s))
		}
	}
}

func TestRunEveryMs(t *testing.T) {
	timesc := make(chan time.Time, 5)
	cron := New()
	cron.Schedule(Every(500*time.Millisecond), FuncJob(func() {
		time.Sleep(490 * time.Millisecond)
		timesc <- time.Now()
	}), "test19")
	cron.Start()
	defer cron.Stop()

	times := []time.Time{}
	for i := 0; i < 5; i++ {
		t := <-timesc
		log.Printf("exec %v", t)
		times = append(times, t)
	}

	assert.InDelta(t, times[1].UnixMilli(), times[0].UnixMilli()+500, 2)
	assert.InDelta(t, times[2].UnixMilli(), times[1].UnixMilli()+500, 2)
	assert.InDelta(t, times[3].UnixMilli(), times[2].UnixMilli()+500, 2)
	assert.InDelta(t, times[4].UnixMilli(), times[3].UnixMilli()+500, 2)
}

func TestRunEveryMsWithSleepOnBoundary(t *testing.T) {
	timesc := make(chan time.Time, 5)
	cron := New()
	cron.Schedule(Every(500*time.Millisecond), FuncJob(func() {
		time.Sleep(501 * time.Millisecond)
		timesc <- time.Now()
	}), "test19")
	cron.Start()
	defer cron.Stop()

	times := []time.Time{}
	for i := 0; i < 5; i++ {
		t := <-timesc
		log.Printf("exec %v", t)
		times = append(times, t)
	}

	assert.InDelta(t, times[1].UnixMilli(), times[0].UnixMilli()+1000, 2)
	assert.InDelta(t, times[2].UnixMilli(), times[1].UnixMilli()+1000, 2)
	assert.InDelta(t, times[3].UnixMilli(), times[2].UnixMilli()+1000, 2)
	assert.InDelta(t, times[4].UnixMilli(), times[3].UnixMilli()+1000, 2)
}

func TestRunTight(t *testing.T) {
	timesc := make(chan time.Time, 5)
	cron := New()
	cron.Schedule(Every(1000*time.Millisecond), FuncJob(func() {
		time.Sleep(900 * time.Millisecond)
		timesc <- time.Now()
	}), "test20")
	cron.Start()
	defer cron.Stop()

	times := []time.Time{}
	for i := 0; i < 5; i++ {
		t := <-timesc
		log.Printf("exec %v", t)
		times = append(times, t)
	}

	assert.InDelta(t, times[1].UnixMilli(), times[0].UnixMilli()+1000, 2)
	assert.InDelta(t, times[2].UnixMilli(), times[1].UnixMilli()+1000, 2)
	assert.InDelta(t, times[3].UnixMilli(), times[2].UnixMilli()+1000, 2)
	assert.InDelta(t, times[4].UnixMilli(), times[3].UnixMilli()+1000, 2)
}

func TestRunTight2(t *testing.T) {
	timesc := make(chan time.Time, 5)
	running := atomic.Bool{}
	cron := New()
	cron.Schedule(Every(500*time.Millisecond), FuncJob(func() {
		if running.Load() {
			return
		}
		running.Store(true)
		defer running.Store(false)

		time.Sleep(1100 * time.Millisecond)
		timesc <- time.Now()
	}), "test20")
	cron.Start()
	defer cron.Stop()

	times := []time.Time{}
	for i := 0; i < 5; i++ {
		t := <-timesc
		times = append(times, t)
		log.Printf("exec %v", t.Sub(times[0]).Milliseconds())
	}

	assert.InDelta(t, times[1].UnixMilli(), times[0].UnixMilli()+1500, 5)
	assert.InDelta(t, times[2].UnixMilli(), times[1].UnixMilli()+1500, 5)
	assert.InDelta(t, times[3].UnixMilli(), times[2].UnixMilli()+1500, 5)
	assert.InDelta(t, times[4].UnixMilli(), times[3].UnixMilli()+1500, 5)
}

// Test that the cron is run in the local time zone (as opposed to UTC).
func TestLocalTimezone(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	now := time.Now().Local()
	spec := fmt.Sprintf("%d %d %d %d %d ?",
		now.Second()+1, now.Minute(), now.Hour(), now.Day(), now.Month())

	cron := New()
	cron.AddFunc(spec, func() { wg.Done() }, "test18", nil)
	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

type testJob struct {
	wg   *sync.WaitGroup
	name string
}

func (t testJob) Run() {
	t.wg.Done()
}

// Simple test using Runnables.
func TestJob(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddJob("0 0 0 30 Feb ?", testJob{wg, "job0"}, "test19", nil)
	cron.AddJob("0 0 0 1 1 ?", testJob{wg, "job1"}, "test20", nil)
	cron.AddJob("* * * * * ?", testJob{wg, "job2"}, "test21", nil)
	cron.AddJob("1 0 0 1 1 ?", testJob{wg, "job3"}, "test22", nil)
	cron.Schedule(Every(5*time.Second+5*time.Nanosecond), testJob{wg, "job4"}, "test23")
	cron.Schedule(Every(5*time.Minute), testJob{wg, "job5"}, "test24")

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
		t.Fatalf("job not run")
	case <-wait(wg):
	}

	// Ensure the entries are in the right order.
	expecteds := []string{"job2", "job4", "job5", "job1", "job3", "job0"}

	var actuals []string
	for _, entry := range cron.Entries() {
		actuals = append(actuals, entry.Job.(testJob).name)
	}

	for i, expected := range expecteds {
		if actuals[i] != expected {
			t.Errorf("Jobs not in the right order.  (expected) %s != %s (actual)", expecteds, actuals)
			t.FailNow()
		}
	}
}

// Add a job, start cron, remove the job, expect it to have not run
func TestAddBeforeRunningThenRemoveWhileRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test25", nil)
	cron.Start()
	defer cron.Stop()
	cron.RemoveJob("test25")

	// Give cron 2 seconds to run our job (which is always activated).
	select {
	case <-time.After(ONE_SECOND):
	case <-wait(wg):
		t.FailNow()
	}
}

// Add a job, remove the job, start cron, expect it to have not run
func TestAddBeforeRunningThenRemoveBeforeRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test26", nil)
	cron.RemoveJob("test26")
	cron.Start()
	defer cron.Stop()

	// Give cron 2 seconds to run our job (which is always activated).
	select {
	case <-time.After(ONE_SECOND):
	case <-wait(wg):
		t.FailNow()
	}
}

func wait(wg *sync.WaitGroup) chan bool {
	ch := make(chan bool)
	go func() {
		wg.Wait()
		ch <- true
	}()
	return ch
}

func stop(cron *Cron) chan bool {
	ch := make(chan bool)
	go func() {
		cron.Stop()
		ch <- true
	}()
	return ch
}

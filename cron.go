// This library implements a cron spec parser and runner.  See the README for
// more details.
package cron

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

type entries []*Entry

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries      entries
	stop         chan struct{}
	running      bool
	work         chan workEntry
	maxWorkers   int
	runningJobs  atomic.Int64
	mu           sync.Mutex
	singleFlight singleflight.Group
}

type workEntry struct {
	f    func()
	name string
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// The Schedule describes a job's duty cycle.
type Schedule interface {
	// Return the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// The schedule on which this job should be run.
	Schedule Schedule

	// The next time the job will run. This is the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	NextScheduleTime time.Time

	// The Job to run.
	Job Job

	// Unique name to identify the Entry so as to be able to remove it later.
	Name string
}

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].NextScheduleTime.IsZero() {
		return false
	}
	if s[j].NextScheduleTime.IsZero() {
		return true
	}
	return s[i].NextScheduleTime.Before(s[j].NextScheduleTime)
}

type Option func(*Cron)

func WithMaxWorkers(maxWorkers int) Option {
	return func(c *Cron) {
		c.maxWorkers = maxWorkers
	}
}

// New returns a new Cron job runner.
func New(opts ...Option) *Cron {
	c := &Cron{
		entries:      nil,
		stop:         make(chan struct{}),
		mu:           sync.Mutex{},
		work:         make(chan workEntry, 2048),
		running:      false,
		maxWorkers:   256,
		runningJobs:  atomic.Int64{},
		singleFlight: singleflight.Group{},
	}
	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *Cron) worker() {
	for {
		select {
		case w := <-c.work:
			// avoid each job being executed multiple times at the same time. DoChan() will discard
			// duplicate calls, so we don't need to check if the job is already running.
			c.singleFlight.DoChan(w.name, func() (interface{}, error) {
				c.runningJobs.Add(1)
				defer c.runningJobs.Add(-1)
				w.f()
				return nil, nil
			})
		case <-c.stop:
			return
		}
	}
}

func (c *Cron) QueuingJobs() int {
	return len(c.work)
}

func (c *Cron) RunningJobs() int {
	return int(c.runningJobs.Load())
}

// FuncJob A wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddFunc(spec string, cmd func(), name string, tz *time.Location) {
	c.AddJob(spec, FuncJob(cmd), name, tz)
}

// AddFunc adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(spec string, cmd Job, name string, tz *time.Location) {
	c.Schedule(Parse(spec, tz), cmd, name)
}

// RemoveJob removes a Job from the Cron based on name.
func (c *Cron) RemoveJob(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	i := c.entries.pos(name)

	if i == -1 {
		return
	}

	c.entries = c.entries[:i+copy(c.entries[i:], c.entries[i+1:])]
}

func (entrySlice entries) pos(name string) int {
	for p, e := range entrySlice {
		if e.Name == name {
			return p
		}
	}
	return -1
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) Schedule(schedule Schedule, cmd Job, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	newEntry := &Entry{
		Schedule: schedule,
		Job:      cmd,
		Name:     name,
	}

	i := c.entries.pos(newEntry.Name)
	if i != -1 {
		return
	}
	c.entries = append(c.entries, newEntry)
	newEntry.NextScheduleTime = newEntry.Schedule.Next(time.Now().Local())
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	return c.sortEntries()
}

// Start the cron scheduler in its own go-routine.
func (c *Cron) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return
	}
	c.running = true

	// Start worker pool
	for i := 0; i < c.maxWorkers; i++ {
		go c.worker()
	}
	go c.run()
}

func (c *Cron) sortEntries() []Entry {
	c.mu.Lock()
	defer c.mu.Unlock()

	sort.Sort(byTime(c.entries))

	entries := []Entry{}
	for _, e := range c.entries {
		entries = append(entries, Entry{
			Schedule:         e.Schedule,
			NextScheduleTime: e.NextScheduleTime,
			Job:              e.Job,
			Name:             e.Name,
		})
	}
	return entries
}

func (c *Cron) bounceNextScheduleTime(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.entries {
		if c.entries[i].Name == name {
			c.entries[i].NextScheduleTime = c.entries[i].Schedule.Next(time.Now().Local())
		}
	}
}

func (c *Cron) step() bool {
	// Determine the next entry to run.
	var effective time.Time
	entries := c.sortEntries()
	if len(entries) == 0 || entries[0].NextScheduleTime.IsZero() {
		// If there are no entries yet, just sleep - it still handles new entries
		// and stop requests.
		effective = time.Now().Local().AddDate(10, 0, 0)
	} else {
		effective = entries[0].NextScheduleTime
	}
	now := time.Now().Local()

	select {
	case <-time.After(effective.Sub(now)):
		// Run every entry whose next time was less than effective time.
		entries := c.sortEntries()
		for _, e := range entries {
			if e.NextScheduleTime.After(effective) {
				return true
			}
			c.bounceNextScheduleTime(e.Name)
			c.work <- workEntry{
				f:    e.Job.Run,
				name: e.Name,
			}
		}
		return true

	case <-c.stop:
		return false

	case <-time.After(time.Second):
		// avoid block
		return true
	}

}

// Run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	// Figure out the next activation times for each entry.
	for _, entry := range c.Entries() {
		c.bounceNextScheduleTime(entry.Name)
	}

	for c.step() {
	}
}

// Stop the cron scheduler.
func (c *Cron) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return
	}

	close(c.stop)
	c.running = false
}

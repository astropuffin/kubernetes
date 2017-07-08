/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"sync"

	"container/heap"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/sets"
	"strconv"
	"runtime"
	"runtime/debug"
)

var PriorityClosedError error = errors.New("DeltaPriority: manipulating with closed queue")

// Priority recieves adds and updates from a Reflector, and puts them in a queue for
// processing based on priorty. If multiple adds/updates of a single item happen while an
// item is in the queue before it has been processed, it will only be processed once, and
// when it is processed, the most recent version will be processed.
//
// Priority solves this use case:
//  * You want to process every object (exactly) once.
//  * You want to process the most recent version of the object when you process it.
//  * You do not want to process deleted objects, they should be removed from the queue.
//  * You need to periodically reorder the queue based on a priority function.
//  Contrast with FIFO or DeltaFIFO for other use cases.

type Priority struct {
	lock sync.RWMutex
	cond sync.Cond
	// This is the underlying queue object without syncronization features
	queue *PriorityQueue

	// populated is true if the first batch of items inserted by Replace() has been populated
	// or Delete/Add/Update was called first.
	populated bool
	// initialPopulationCount is the number of items inserted by the first call of Replace()
	initialPopulationCount int

	// keyFunc is used to make the key used for queued item insertion and retrieval, and
	// should be deterministic.
	// TODO: is this needed at the top level object?
	keyFunc KeyFunc

	// Indication the queue is closed.
	// Used to indicate a queue is closed so a control loop can exit when a queue is empty.
	// Currently, not used to gate any of CRED operations.
	closed     bool
	closedLock sync.Mutex
}

type PriorityKey struct {
	key      string
	priority int
	index    int
}

type PriorityQueue struct {
	//this is the underlying priority queue object
	queue []PriorityKey
	items map[string]interface{}

	// keyFunc is used to make the key used for queued item insertion and retrieval, and
	// should be deterministic.
	keyFunc KeyFunc
}

var (
	_ = Queue(&Priority{}) // Priority is a Queue
)

func (p *Priority) Close() {
	trace()
	glog.V(4).Infof("priority.Close: %v", string(debug.Stack()[:]))
	p.closedLock.Lock()
	defer p.closedLock.Unlock()
	p.closed = true
	p.cond.Broadcast()
}

// HasSynced
// returns true if an Add/Update/Delete/AddIfNotPresent are called first,
// or an Update called first but the first batch of items inserted by Replace() has been popped
func (p *Priority) HasSynced() bool {
	trace()
	glog.V(4).Infof("priority.HasSynced: %v", string(debug.Stack()[:]))
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.populated && p.initialPopulationCount == 0
}

// Init functions

// New PriorityQueue returns a struct that can be used to store items
// to be retrieved in priority order
func NewPriorityQueue(keyFunc KeyFunc) *PriorityQueue {
	trace()
	glog.V(4).Infof("NewPriorityQueue: %v", string(debug.Stack()[:]))
	pq := &PriorityQueue{
		items:   map[string]interface{}{},
		queue:   []PriorityKey{},
		keyFunc: keyFunc,
	}
	heap.Init(pq)
	return pq
}

// NewPriority returns a Store which can be used to queue up items to
// process.
func NewPriority(keyFunc KeyFunc) *Priority {
	trace()
	glog.V(4).Infof("NewPriority: %v", string(debug.Stack()[:]))
	p := &Priority{
		queue:   NewPriorityQueue(keyFunc),
		keyFunc: keyFunc, //TODO: is this needed here?
	}

	p.cond.L = &p.lock
	return p
}

// Helper Methods
func (pq PriorityQueue) GetPlaceInQueue(key string) (int, error) {
	trace()
	glog.V(4).Infof("priorityqueue.GetPlaceInQueue: %v", string(debug.Stack()[:]))
	for i, pk := range pq.queue {
		if pk.key == key {
			return i, nil
		}
	}
	return -1, errors.New("key not found in queue")
}

// Methods for the heap interface:

// Len
// returns the length of the queue
func (pq PriorityQueue) Len() int {
	trace()
	glog.V(4).Infof("priorityqueue.Len: %v", string(debug.Stack()[:]))
	return len(pq.queue)
}

// Less
// compares the relative priority of two items in the queue and
// for a priority queue, less is more
func (pq PriorityQueue) Less(i, j int) bool {
	trace()
	glog.V(4).Infof("priorityqueue.Less: %v", string(debug.Stack()[:]))
	//Pop should give us the highest priority item
	return pq.more(i, j)
}

// more
func (pq PriorityQueue) more(i, j int) bool {
	trace()
	glog.V(4).Infof("priorityqueue.more: %v", string(debug.Stack()[:]))
	return pq.queue[i].priority > pq.queue[j].priority
}

// Swap
// switches the position of two items in the queue
func (pq PriorityQueue) Swap(i, j int) {
	trace()
	glog.V(4).Infof("priorityqueue.Swap: %v", string(debug.Stack()[:]))
	pq.queue[i], pq.queue[j] = pq.queue[j], pq.queue[i]
	pq.queue[i].index = i
	pq.queue[j].index = j
}

// Push
// adds an item to the queue. Used by the heap function. Do not use this
// outside heap.Push!
func (pq *PriorityQueue) Push(obj interface{}) {
	trace()
	glog.V(4).Infof("priorityqueue.Push: %v", string(debug.Stack()[:]))
	//debug
	_, _ = MetaRateLimitFunc(obj)
	key, _ := pq.keyFunc(obj)
	priority, _ := MetaPriorityFunc(obj)
	n := pq.Len()
	pk := PriorityKey{
		key:      key,
		priority: priority,
		index:    n,
	}

	pq.items[key] = obj
	pq.queue = append(pq.queue, pk)
}

// Pop
// grabs the highest priority item from the queue, returns and deletes it
// Do not use outside of heap.Pop!
func (pq *PriorityQueue) Pop() interface{} {
	trace()
	glog.V(4).Infof("priorityqueue.Pop: %v", string(debug.Stack()[:]))
	//grab the queue
	old := pq.queue
	n := len(old)
	pk := old[n-1]
	item := pq.items[pk.key]
	glog.V(4).Infof("priority pop: %v", item)
	glog.V(4).Infof("current queue %#v", pq.items)

	//delete from map and array
	delete(pq.items, pk.key)
	pq.queue = old[0 : n-1]

	priority, _ := MetaPriorityFunc(item)
	glog.V(4).Infof("priority: '%v'", priority)
	return item
}

////update
////isn't actually needed by the interface
//func (pq *Priority) update(item *PriorityKey, priority int) {
//    item.priority = priority
//    heap.Fix(pq.queue, item.index)
//}

// Methods for the Store interface:

// Add
// inserts an item, and puts it in the queue. The item is only enqueued
// if it doesn't already exist in the set.
func (p *Priority) Add(obj interface{}) error {
	trace()
	glog.V(4).Infof("priority.Add: %v", string(debug.Stack()[:]))
	return p.AddIfNotPresent(obj)
}

// Update
// can modify any part of the object, especially its priority
// However, if the keyfunc result is not identical to the original object, then
// this acts like Add. The controller currently creates the key based on
// GetNamespace() and GetLabels(), so it should be safe to update anything
// else.
func (p *Priority) Update(obj interface{}) error {
	trace()
	glog.V(4).Infof("priority.Update: %v", string(debug.Stack()[:]))
	key, err := p.keyFunc(obj)
	pq := p.queue

	//if it already exists, then update the object and don't add a new key
	if _, exists := pq.items[key]; exists {
		pq.items[key] = obj
		//the item is already indexed, but might need a new priority
		index, _ := pq.GetPlaceInQueue(key)
		_, _ = MetaRateLimitFunc(obj)
		priority, _ := MetaPriorityFunc(obj)

		pq.queue[index].priority = priority
		heap.Fix(pq, index)
	} else {
		//if it doesn't already exist (or it has a new key), then add it
		heap.Push(pq, obj) //I hope this actually works...
	}
	//TODO: fix error handling
	return err
}

// Delete
// Delete removes an item from anywhere in the queue
// TODO: move the bulk of this to the PriorityQueue object
func (p *Priority) Delete(obj interface{}) error {
	trace()
	glog.V(4).Infof("priority.Delete: %v", string(debug.Stack()[:]))
	key, err := p.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	p.populated = true

	pq := p.queue
	delete(pq.items, key)
	i, err := pq.GetPlaceInQueue(key)

	if i != -1 {
		pq.queue = append(pq.queue[:i], pq.queue[i+1:]...)
	}

	return err
}

// List
// List returns an array of all the items in priority order
func (p *Priority) List() []interface{} {
	trace()
	glog.V(4).Info("priority.List")
	p.lock.RLock()
	defer p.lock.RUnlock()
	pq := p.queue
	list := make([]interface{}, 0, len(pq.items))

	for i := len(pq.items) - 1; i >= 0; i-- {
		list = append(list, pq.items[pq.queue[i].key])
	}
	return list
}

// ListKeys
// ListKeys returns a list of all the keys of the objects currently
// in the Priority. This is NOT sorted by priority. //TODO: Should it be?
func (p *Priority) ListKeys() []string {
	trace()
	glog.V(4).Info("priority.ListKeys")
	p.lock.RLock()
	defer p.lock.RUnlock()
	pq := p.queue
	list := make([]string, 0, len(pq.items))
	for item := range pq.items {
		list = append(list, item)
	}
	return list
}

// Get
// Get returns the requested item, or sets exists=false.
func (p *Priority) Get(obj interface{}) (item interface{}, exists bool, err error) {
	trace()
	glog.V(4).Info("priority.Get")
	key, err := p.keyFunc(obj)
	if err != nil {
		return nil, false, KeyError{obj, err}
	}
	return p.GetByKey(key)
}

// GetByKey
// GetByKey returns the requested item, or sets exists=false.
func (p *Priority) GetByKey(key string) (item interface{}, exists bool, err error) {
	trace()
	glog.V(4).Info("priority.GetByKey")
	p.lock.RLock()
	defer p.lock.RUnlock()
	pq := p.queue
	item, exists = pq.items[key]
	return item, exists, nil
}

func (p *Priority) IsClosed() bool {
	trace()
	glog.V(4).Info("priority.IsClosed")
	p.closedLock.Lock()
	defer p.closedLock.Unlock()
	if p.closed {
		return true
	}
	return false
}

// Replace
// Replace will delete the contents of 'p', using instead the given map.
// 'p' takes ownership of the map, you should not reference the map again
// after calling this function. p's queue is reset, too; upon return, it
// will contain the items in the map, in priority order.
func (p *Priority) Replace(list []interface{}, resourceVersion string) error {
	trace()
	glog.V(4).Info("priority.Replace")

	pq := NewPriorityQueue(p.keyFunc)
	for _, item := range list {
		heap.Push(pq, item)
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if !p.populated {
		p.populated = true
		p.initialPopulationCount = pq.Len()
	}

	p.queue = pq

	if p.queue.Len() > 0 {
		p.cond.Broadcast()
	}
	return nil
}

// Resync
// Resync will make sure all the items in the object map are in the queue
// it currently doesn't check if all items in the queue are in the map, so
// there could be dangling items in the queue... //TODO
func (p *Priority) Resync() error {
	trace()
	glog.V(4).Info("priority.Rsync")
	p.lock.Lock()
	defer p.lock.Unlock()

	pq := p.queue
	inQueue := sets.NewString()
	for _, pk := range pq.queue {
		inQueue.Insert(pk.key)
	}
	for key, item := range pq.items {
		if !inQueue.Has(key) {
			heap.Push(pq, item)
		}
	}
	if len(pq.queue) != len(pq.items) {
		return errors.New("PriorityQueue failed to sync")
	}

	if p.queue.Len() > 0 {
		p.cond.Broadcast()
	}
	return nil
}

// Methods for the Queue interface:

// Pop
// Pop waits until an item is ready and processes it. If multiple items are
// ready, the highest priority one is returned.
// The item is removed from the queue (and the store) before it is processed,
// so if you don't successfully process it, it should be added back with
// AddIfNotPresent(). process function is called under lock, so it is safe
// update data structures in it that need to be in sync with the queue.
func (p *Priority) Pop(process PopProcessFunc) (interface{}, error) {
	trace()
	glog.V(4).Info("priority.Pop")
	p.lock.Lock()
	defer p.lock.Unlock()
	for { //what is this forever loop for? To cycle through items until one is ready?
		// When the queue is empty, invocation of Pop() is blocked until new item is enqueued.
		// When Close() is called, the p.closed is set and the condition is broadcasted.
		// Which causes this loop to continue and return from the Pop().
		if p.IsClosed() {
			return nil, PriorityClosedError
		}

		for p.queue.Len() == 0 {
			p.cond.Wait()
		}

		var item interface{}
		var rateLimitedPods []interface{}
		//pop pods from the heap until we find one that isn't rateLimited
		for {
			if p.queue.Len() == 0 {
				break
			}
			item = heap.Pop(p.queue)

			rateLimited, err := MetaRateLimitFunc(item)
			if err != nil {
				glog.V(4).Info(err)
			}
			glog.V(4).Infof("ratelimited?: %v", rateLimited)
			if rateLimited {
				glog.V(4).Infof("ratelimited pod, temporarily removing from queue: %v",item)
				rateLimitedPods = append(rateLimitedPods, item)
				item = nil
			} else {
				break
			}
		}

		// add rate limited pods back into the priority queue
		for pod, _ := range rateLimitedPods {
			glog.V(4).Infof("ratelimited pod, adding back to queue: %v",pod)
			p.AddIfNotPresent(pod)
		}

		//if we went through the whole list and only found rateLimited pods, go back for another loop
		if item == nil {
			continue
		}

		if p.initialPopulationCount > 0 {
			p.initialPopulationCount--
		}

		err := process(item)
		//TODO: this never returns anything but nil currently...
		glog.V(4).Infof("item is: '%v'", item)
		glog.V(4).Infof("requeue?: '%#v'", err)
		if e, ok := err.(ErrRequeue); ok {
			//key, _ := p.keyFunc(item) //TODO: add error handling
			//p.addIfNotPresent(key, item)
			p.AddIfNotPresent(item)
			err = e.Err
		}
		return item, err
	}
}

// AddIfNotPresent
// inserts an item, and puts it in the queue. If the item is already
// present in the set, it is neither enqueued nor added to the set.
//
// This is useful in a single producer/consumer scenario so that the consumer can
// safely retry items without contending with the producer and potentially enqueueing
// stale items.
// syncronized wrapper for addIfNotPresent. It also derives the key
// in order to avoid unneccessary locks
func (p *Priority) AddIfNotPresent(obj interface{}) error {
	trace()
	glog.V(4).Info("priority.AddIfNotPresent")
	key, err := p.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	p.addIfNotPresent(key, obj)

	return nil
}

// addIfNotPresent
// this is the non syncronized form. It should always be called under lock
func (p *Priority) addIfNotPresent(key string, obj interface{}) {
	trace()
	glog.V(4).Info("priority.addIfNotPresent")
	glog.V(4).Infof("addIfNotPresent -- key: %v, value %v",key, obj)
	p.populated = true
	pq := p.queue
	//here's where the map is important...
	if _, exists := pq.items[key]; !exists {
		heap.Push(pq, obj)
	}
	p.cond.Broadcast()
}


// Helper Functions
const annotationKey = "dronedeploy.com/priority"

// MetaPriorityFunc
// extracts the priority annotation of an object
// if the priority is not set, then set priority to -1
// The object must be a pointer of a valid API type
// TODO: make this allow non-pod objects
func MetaPriorityFunc(obj interface{}) (int, error) {
	trace()
	glog.V(4).Info("MetaPriorityFunc")
	thing, err := meta.Accessor(obj)
	if err != nil {
		return -1, fmt.Errorf("object has no meta: %v", err)
	}
	annotations := thing.GetAnnotations()
	if annotations == nil {
		return -1, fmt.Errorf("object does not have annotations")
	}
	if p, ok := annotations[annotationKey]; ok {
		glog.V(4).Infof("priority annotation: '%v'", p)
		priority, err := strconv.Atoi(p)
		if err != nil {
			return -1, fmt.Errorf("priority is not an integer: %q", p)
		}
		return priority, nil
	}
	return -1, nil
}

const rateLimitAnnotationKey = "dronedeploy.com/rateLimit"

// MetaRateLimitFunc
// extracts the rateLimit annotation of an object.
// defaults to false (not rate limited)
// The object must be a pointer of a valid API type
// TODO: make this allow non-pod objects
func MetaRateLimitFunc(obj interface{}) (bool, error) {
	trace()
	glog.V(4).Info("MetaRateLimitFunc")
	thing, err := meta.Accessor(obj)
	if err != nil {
		return false, fmt.Errorf("object has no meta: %v", err)
	}
	annotations := thing.GetAnnotations()
	if annotations == nil {
		return false, fmt.Errorf("object does not have annotations")
	}
	if rl, ok := annotations[rateLimitAnnotationKey]; ok {
		glog.V(4).Infof("rateLimit annotation: '%v'", rl)
		if rl == "true" {
			glog.V(4).Infof("rateLimiting %v", thing.GetName())
			return true, nil
		}
	}
	glog.V(4).Infof("not rateLimiting %v", thing.GetName())
	return false, nil
}

func trace() {
	pc := make([]uintptr, 10)  // at least 1 entry needed
	runtime.Callers(2, pc)
	f := runtime.FuncForPC(pc[0])
	file, line := f.FileLine(pc[0])
	fmt.Printf("%s:%d %s\n", file, line, f.Name())
}

// There are a number of objects pulled from other files in the package. Here
// are a few important ones:
// PopProcessFunc, ErrRequeue

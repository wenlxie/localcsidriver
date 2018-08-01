/*
Copyright 2018 The Kubernetes Authors.

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

package pendingoperations

import (
	"sync"

	"github.com/kubernetes-csi/localcsidriver/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Operation contains the operation that is created as well as
// supporting functions required for driver interfaces.
type Operation struct {
	Type          OperationType
	OperationFunc func() (err error)
	CompleteFunc  func(*error)
}

type OperationType string

const(
	CreateOperation OperationType = "CreateVolume"
	DeleteOperation OperationType = "DeleteVolume"
	NodeStageOperation OperationType = "NodeStage"
	NodeUnstageOperation OperationType = "NodeUnstage"
	NodePublishOperation OperationType = "NodePublish"
	NodeUnpublishOperation OperationType = "NodeUnpublish"
)

// NestedPendingOperations defines the supported set of operations.
type PendingOperations interface {
	// Run adds the concatenation of volumeName to the list of running
	// operations and spawns a new go routine to execute operationFunc.
	// If an operation with the same volumeName exists, an AlreadyExists
	// error is returned.
	// Once the operation is complete, the go routine is terminated and the
	// concatenation of volumeName is removed from the list of
	// executing operations allowing a new operation to be started with the
	// volumeName without error.
	Run(volumeName string, operation *Operation) error

	// Wait blocks until all operations are completed. This is typically
	// necessary during tests - the test should wait until all operations finish
	// and evaluate results after that.
	Wait()

	// HasPendingOperation returns true and the operation if we have
	// an operation for the given volumeName, otherwise it returns false
	HasPendingOperation(volumeName string) (*Operation, bool)
}

// New returns a new instance of PendingOperations.
func New() PendingOperations {
	g := &pendingOperations{
		operations: map[string]*Operation{},
	}
	g.cond = sync.NewCond(&g.lock)
	return g
}

type pendingOperations struct {
	operations map[string]*Operation
	cond       *sync.Cond
	lock       sync.RWMutex
}

func (po *pendingOperations) Run(volumeName string, operation *Operation) error {
	po.lock.Lock()
	exstingOp, opExists := po.hasPendingOperation(volumeName)
	if opExists {
		po.lock.Unlock()
		return NewAlreadyExistsError(volumeName, exstingOp.Type)
	}

	po.operations[volumeName] = operation
	po.lock.Unlock()

	errCh := make(chan error, 1)
	go func() (opErr error) {
		// Handle unhandled panics (very unlikely)
		defer util.HandleCrash()
		// Handle completion
		defer po.operationComplete(volumeName)
		if operation.CompleteFunc != nil {
			defer operation.CompleteFunc(&opErr)
		}
		// Handle panic, if any, from operationFunc()
		defer util.RecoverFromPanic(&opErr)
		opErr = operation.OperationFunc()
		errCh <- opErr

		return
	}()

	return <-errCh
}

func (po *pendingOperations) operationComplete(volumeName string) {
	// Defer operations are executed in Last-In is First-Out order. In this case
	// the lock is acquired first when operationCompletes begins, and is
	// released when the method finishes, after the lock is released cond is
	// signaled to wake waiting goroutine.
	defer po.cond.Signal()
	po.lock.Lock()
	defer po.lock.Unlock()

	delete(po.operations, volumeName)
}

func (po *pendingOperations) HasPendingOperation(volumeName string) (*Operation, bool) {
	po.lock.RLock()
	defer po.lock.RUnlock()

	return po.hasPendingOperation(volumeName)
}

func (po *pendingOperations) hasPendingOperation(volumeName string) (*Operation, bool) {
	operation, exist := po.operations[volumeName]
	if exist {
		return operation, true
	}

	return nil, false
}

func (po *pendingOperations) Wait() {
	po.lock.Lock()
	defer po.lock.Unlock()

	for len(po.operations) > 0 {
		po.cond.Wait()
	}
}

// NewAlreadyExistsError returns a new instance of AlreadyExists error.
func NewAlreadyExistsError(volumeName string, opType OperationType) error {
	return status.Errorf(codes.AlreadyExists, "%s operation for volume %s still ongoing", opType, volumeName)
}

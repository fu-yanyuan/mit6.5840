package lock

import (
	"log"
	"time"
	// "fmt"
	"sync"

	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck 			kvtest.IKVClerk
	// You may add code here
	lockKey		string
	clientID 	string
	mu 		sync.Mutex
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck: ck,
		lockKey: l,
		clientID: kvtest.RandValue(8),
	}

	// fmt.Printf("lockKey: %v, clientID: %v \n", l, lk.clientID)
	return lk
}

func (lk *Lock) Acquire() {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	// Your code here
	// keep trying to acquire the lock
	// fmt.Printf("== %v try to acquire the key \n", lk.clientID)
	for {
		val, _, err := lk.ck.Get(lk.lockKey)
		// fmt.Printf("val = %v, err = %v \n", val, err)

		if err == rpc.ErrNoKey {
			// no such key, we can lock it
			putErr := lk.ck.Put(lk.lockKey, lk.clientID, 0)
			if putErr == rpc.OK {
				return
			}
		} else if err == rpc.OK {
			// no one is holding this key, we can lock it
			if val == "" {
				if lk.ck.Put(lk.lockKey, lk.clientID, 0) == rpc.OK {
					return
				}
			}
			// or we already locked
			if val == lk.clientID {
				return
			}

			// key is locked by some other clent
			time.Sleep(10 * time.Millisecond)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (lk *Lock) Release() {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	
	// Your code here
	// fmt.Printf("== %v try to release the key \n", lk.clientID)
	val, ver, err := lk.ck.Get(lk.lockKey)
	if err != rpc.OK {
		return
		// log.Fatalf("lock key not found")
	}

	if val != lk.lockKey {
		return
		// log.Fatalf("lock held by another client")
	}

	// err == rpc.OK && val == lk.lockKey
	putErr := lk.ck.Put(lk.lockKey, "", ver)
	if putErr != rpc.OK {
		log.Fatalf("lk.ck.Put Err: %v", putErr)
	}
	// fmt.Printf("== %v released the key \n", lk.clientID)
}

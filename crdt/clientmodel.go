package crdt

import (
	"strconv"
	"strings"
)

var SEPARATOR = "####"

var REMOVAL_VALUE = "-1_REMOVAL"

type LWWElementSet struct {

	// We keep different set of arrays to keep the payload because of concurrency issues.
	// But we do only keep the latest copy of the operation performed on a key 'k'.
	// The size of any of the operations array will be bound by the size of the number of keys
	// in the element set.

	// It can be possible that an update comes before an add, but "Eventually" the set will
	// have the same data across all the replicas.

	// In all the arrays we store the key as - {"key_value" : "value####timestamp_string"}

	// Initial set will be provided by the application, only if it already exists,
	// else it will be empty set. For consistency, even for creation, we query new Application for it.
	// Which inturn will return an empty map.
	InitialSet			map[string]string

	addOperations		map[string]string
	removeOperations	map[string]string
}


func CreateLWWElementSet() *LWWElementSet {
	return &LWWElementSet{
		InitialSet:       make(map[string]string),
		addOperations:    make(map[string]string),
		removeOperations: make(map[string]string),
	}
}

func CreateLWWElementSetWithInitial(initialSet map[string]string) *LWWElementSet {
	return &LWWElementSet{
		InitialSet:       initialSet,
		addOperations:    make(map[string]string),
		removeOperations: make(map[string]string),
	}
}


/**
These methods will be called by client, and not directly,

The Add and Update can be unified with a implementation like Upsert.
We provide the APIs as such, but implement them in an upsert way.
*/
func (lww *LWWElementSet) UpsertElement(payload Payload) {

	if val2, ok := lww.addOperations[payload.key]; ok {
		// we can throw an error if add is performed on a key which already exists.
		// Can check the payload.op here.

		// Key exists, we update the element set with whichever new timestamp is.

		lww.addOperations[payload.key] = GetLatestValueTimestamp(payload.val, val2)

	} else {
		// we can throw an error if update is performed on a key which doesn't exists.
		// Can check the payload.op here.

		// Key doesn't exist, simple insert would do.
		// The timestamp is already the part of this payload.val and separated by '####'
		lww.addOperations[payload.key] = payload.val
	}
}

func (lww *LWWElementSet) RemoveElement(payload Payload) {
	// All remove elements will have "-1_removal####timestamp" as value of a key.
	// This makes it consistent with Payload struct.


	vals := strings.Split(payload.val, SEPARATOR)
	payload.val = REMOVAL_VALUE + SEPARATOR + vals[1]

	if val2, ok := lww.removeOperations[payload.key]; ok {
		// Key exists, we update the element set with whichever new timestamp is.

		lww.removeOperations[payload.key] = GetLatestValueTimestamp(payload.val, val2)

	} else {
		// Key doesn't exist, simple insert would do.
		// The timestamp is already the part of this payload.val and separated by '####'
		lww.removeOperations[payload.key] = payload.val
	}
}

func (lww *LWWElementSet) ViewElements() map[string]string {

	// We iterate addOperations because the result can only be a subset of adds.
	// As remove operations even though can have more keys (in cases) will only remove the
	// elements from the result.

	result := make(map[string]string)

	for key, val := range lww.addOperations {

		val1 := val

		if val2, ok := lww.removeOperations[key]; ok {
			// If the latest value is not REMOVAL_STRING, but val1 from add Ops.
			if GetLatestValueTimestamp(val1, val2) == val1 {
				result[key] = strings.Split(val1, SEPARATOR)[0]
			}
		} else {
			// In case we dont have any removal operation related to this key.
			result[key] = strings.Split(val1, SEPARATOR)[0]
		}
	}

	return result
}

func GetLatestValueTimestamp(val1 string, val2 string) string {

	val1s := strings.Split(val1, SEPARATOR)
	timestamp1, _ := strconv.Atoi(val1s[1])

	val2s := strings.Split(val2, SEPARATOR)
	timestamp2, _ := strconv.Atoi(val2s[1])

	if timestamp2 > timestamp1 {
		return val2
	} else {
		return val1
	}
}
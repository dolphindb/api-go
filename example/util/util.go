package util

import (
	"fmt"
	"reflect"
)

// AssertNil checks whether the error is nil
// if not, panic.
func AssertNil(err error) {
	if err != nil {
		panic(fmt.Sprintf("err is not nil: %s", err.Error()))
	}
}

// AssertEqual checks whether the s is equal to the d
// if not, panic.
func AssertEqual(s, d interface{}) {
	if !reflect.DeepEqual(s, d) {
		panic(fmt.Sprintf("%v != %v", s, d))
	}
}

// AssertTrue checks whether the s is true
// if not, panic.
func AssertTrue(s bool) {
	if !s {
		panic(fmt.Sprintf("%v is not equal true", s))
	}
}

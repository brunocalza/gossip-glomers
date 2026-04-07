package main

import (
	"fmt"
	"testing"
	"time"
)

func TestSnowflake(t *testing.T) {
	// 2026-04-07T12:55:09+00:00
	generator := NewSnowflakeGenerator(0, NewFakeClock(time.Unix(1775566509, 0)))

	n := generator.Next()
	fmt.Println(n)
	assert(t, n, 1775566509000, 0, 0)

	// increment 10 in the same millisecond
	for i := 0; i < 10; i++ {
		n = generator.Next()
	}
	assert(t, n, 1775566509000, 0, 10)

}

func TestSnowflakeSeqOverflow(t *testing.T) {
	// 2026-04-07T12:55:09+00:00
	generator := NewSnowflakeGenerator(0, NewFakeClock(time.Unix(1775566509, 0)))

	n := generator.Next()
	assert(t, n, 1775566509000, 0, 0)

	// increment 4095 in the same millisecond
	for i := 0; i < 4095; i++ {
		n = generator.Next()
	}
	assert(t, n, 1775566509000, 0, 4095)

	// the next number overflows seq, so we need to wait for the next millisecond
	n = generator.Next()
	assert(t, n, 1775566509001, 0, 0)
}

func TestSnowflakeDifferentMillisecond(t *testing.T) {
	clock := NewFakeClock(time.Unix(1775566509, 0))

	// 2026-04-07T12:55:09+00:00
	generator := NewSnowflakeGenerator(0, clock)

	n := generator.Next()
	assert(t, n, 1775566509000, 0, 0)

	clock.Sleep(time.Millisecond)

	// seq will be 0 again but timestamp increased

	n = generator.Next()
	assert(t, n, 1775566509001, 0, 0)

}

func TestSnowflakeDifferentMachines(t *testing.T) {

	// 2026-04-07T12:55:09+00:00
	generator1 := NewSnowflakeGenerator(0, NewFakeClock(time.Unix(1775566509, 0)))
	generator2 := NewSnowflakeGenerator(0, NewFakeClock(time.Unix(1775566509, 0)))

	n1 := generator1.Next()
	assert(t, n1, 1775566509000, 0, 0)

	n2 := generator2.Next()
	assert(t, n2, 1775566509000, 0, 0)
}

func getBits(x int64, start, length uint) int64 {
	shift := 64 - start - length
	mask := int64((1 << length) - 1)
	return (x >> shift) & mask
}

func assert(t *testing.T, n int64, timestamp int64, machineId int64, seq int64) {
	expTimestamp := getBits(n, 1, 41) + customEpoch
	if expTimestamp != timestamp {
		t.Errorf("expected %d got %d", expTimestamp, timestamp)
	}

	expMachineId := getBits(n, 42, 10)
	if expMachineId != machineId {
		t.Errorf("expected %d got %d", expMachineId, machineId)
	}

	expSeq := getBits(n, 52, 12)
	if expSeq != seq {
		t.Errorf("expected %d got %d", expSeq, seq)
	}

}

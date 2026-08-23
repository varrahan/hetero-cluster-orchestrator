package main

import "testing"

func TestTransactionOwnershipAndBounds(t *testing.T) {
	server := &server{sharedRoot: t.TempDir(), clusterUID: "cluster", budget: 64 << 20, transactions: map[string]*transaction{}, active: map[string]string{}, runOwners: map[string]uint64{}}
	id := "0123456789abcdef0123456789abcdef"
	result, err := server.createTransaction(id, 10, 0, createTransactionRequest{Streams: []streamRequest{{Name: "upload", ByteLength: 17 << 20}}})
	if err != nil || result.SlotBytes < 1<<20 {
		t.Fatalf("create transaction = %#v, %v", result, err)
	}
	if _, err := server.transactionStream(id, "upload", 11, 0); err == nil {
		t.Fatal("different job accessed transaction")
	}
	if _, err := server.createTransaction("ffffffffffffffffffffffffffffffff", 10, 0, createTransactionRequest{Streams: []streamRequest{{Name: "other", ByteLength: 1}}}); err == nil {
		t.Fatal("parallel transaction was accepted")
	}
	if err := server.deleteTransaction(id, 10, 0); err != nil {
		t.Fatal(err)
	}
}

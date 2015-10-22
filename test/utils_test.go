package main

import (
	"testing"
	"utils"
)

func TestInList(t *testing.T) {
	vals := []string{"1", "2", "3"}
	res := utils.InList("1", vals)
	if res == false {
		t.Error("Fail TestInList")
	}
	res = utils.InList("5", vals)
	if res == true {
		t.Error("Fail TestInList")
	}
}

func TestCheckEntries(t *testing.T) {
	list1 := []string{"1", "2"}
	list2 := []string{"1", "2", "3"}
	res := utils.CheckEntries(list1, list2)
	if res == false {
		t.Error("Fail TestCheckEntries")
	}
	list1 = []string{"1"}
	list2 = []string{"2", "3"}
	res = utils.CheckEntries(list1, list2)
	if res == true {
		t.Error("Fail TestCheckEntries")
	}
}

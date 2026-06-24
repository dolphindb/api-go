package model

import "testing"

func mustNewTable(t *testing.T, colNames []string, colValues []*Vector) *Table {
	t.Helper()
	tb, err := NewTable(colNames, colValues)
	if err != nil {
		t.Fatal(err)
	}
	return tb
}

func mustNewPair(t *testing.T, v *Vector) *Pair {
	t.Helper()
	p, err := NewPair(v)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func mustNewSet(t *testing.T, v *Vector) *Set {
	t.Helper()
	s, err := NewSet(v)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func mustNewMatrix(t *testing.T, data, rowLabels, columnLabels *Vector) *Matrix {
	t.Helper()
	m, err := NewMatrix(data, rowLabels, columnLabels)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func mustNewDictionary(t *testing.T, keys, values *Vector) *Dictionary {
	t.Helper()
	d, err := NewDictionary(keys, values)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func mustNewChart(t *testing.T, in map[string]DataForm) *Chart {
	t.Helper()
	ch, err := NewChart(in)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

func mustNewVectorWithArrayVector(t *testing.T, data []*ArrayVector) *Vector {
	t.Helper()
	v, err := NewVectorWithArrayVector(data)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

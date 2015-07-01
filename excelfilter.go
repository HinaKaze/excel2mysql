package main

import (
	"github.com/tealeg/xlsx"
)

type excelFilter struct {
	filename                   string
	cBegin, cEnd, rBegin, rEnd int
}

func (eFilter *excelFilter) Rows(rawRows []*xlsx.Row) []*xlsx.Row {
	if eFilter.rBegin <= 0 || eFilter.rEnd <= 0 {
		return rawRows
	}
	if eFilter.rBegin > eFilter.rEnd {
		return rawRows
	}
	return rawRows[eFilter.rBegin-1 : eFilter.rEnd]
}

func (eFilter *excelFilter) Cells(rawCells []*xlsx.Cell) []*xlsx.Cell {
	if eFilter.cBegin <= 0 || eFilter.cEnd <= 0 {
		return rawCells
	}
	if eFilter.cBegin > eFilter.cEnd {
		return rawCells
	}
	return rawCells[eFilter.cBegin-1 : eFilter.cEnd]
}

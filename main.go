package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/HinaKaze/initparse"
	"github.com/go-sql-driver/mysql"
	"github.com/tealeg/xlsx"
	"os"
	"path/filepath"
	"strings"
)

var excelRoot string = "./excel/"
var exportRoot string = "./txt/"

//var dbstr string = "cehua:cehua123@tcp(10.10.0.100:3306)/fr_game"

var filterMap map[string]*excelFilter = make(map[string]*excelFilter)
var conns []*sql.DB = make([]*sql.DB, 0)

func main() {
	loadConfig()
	excelNames := getExcelList()
	for _, fName := range excelNames {
		fmt.Println("Excel file name", fName)
		iFile, err := xlsx.OpenFile(fName)
		if err != nil {
			panic(err.Error())
		}
		fileName := fName[strings.LastIndex(fName, "\\")+1 : strings.Index(fName, ".")]
		exportFileName := exportRoot + fileName + ".txt"
		fmt.Println("Export file name", exportFileName)
		err = os.Remove(exportFileName)
		if err != nil {
			//panic(err.Error())
		}
		eFile, err := os.Create(exportFileName)
		if err != nil {
			panic(err.Error())
		}
		defer eFile.Close()
		//get filter
		excelFilter, ok := filterMap[fileName]
		if !ok {
			excelFilter = filterMap["default"]
		}
		export(iFile, eFile, excelFilter)
		for i := 0; i < len(conns); i++ {
			import2db(conns[i], eFile, fileName)
		}
	}
	for _, dbconn := range conns {
		dbconn.Close()
	}
}

func loadConfig() {
	initparse.DefaultParse("./config.cfg")

	if paths, ok := initparse.GetSection("Path"); ok {
		excelRoot, _ = paths.GetValue("excelRoot")
		exportRoot, _ = paths.GetValue("exportRoot")
	}
	//load filter
	filterMap["default"] = &excelFilter{"default", -1, -1, -1, -1}
	for i := 1; i < 1024; i++ {
		sectionName := "Filter" + fmt.Sprintf("%d", i)
		if filters, ok := initparse.GetSection(sectionName); ok {
			filename, _ := filters.GetValue("filename")
			rBegin := filters.GetIntValue("rBegin")
			rEnd := filters.GetIntValue("rEnd")
			cBegin := filters.GetIntValue("cBegin")
			cEnd := filters.GetIntValue("cEnd")

			filterMap[filename] = &excelFilter{filename, cBegin, cEnd, rBegin, rEnd}
		} else {
			break
		}
	}
	//load db conn
	for i := 1; i < 1024; i++ {
		sectionName := "DB" + fmt.Sprintf("%d", i)
		if connSection, ok := initparse.GetSection(sectionName); ok {
			connStr, ok := connSection.GetValue("ConnStr")
			fmt.Printf("Connect db [%s]\n", connStr)
			if !ok {
				fmt.Printf("[Warning] Section %s has no property ConnStr\n", sectionName)
				continue
			}
			sqlconn, err := sql.Open("mysql", connStr)
			if err != nil {
				fmt.Printf("[Warning] Error in open sql conn [%s]\n", connStr)
				continue
			}
			conns = append(conns, sqlconn)
		} else {
			break
		}
	}
}

func getExcelList() (fileNames []string) {
	filepath.Walk(excelRoot, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		fileNames = append(fileNames, path)
		return nil
	})
	return
}

//默认使用第一个sheet
func export(excelFile *xlsx.File, exportFile *os.File, filter *excelFilter) {
	sheet := excelFile.Sheets[0]
	var isFirstRow bool = false
	for _, row := range filter.Rows(sheet.Rows) {
		if !isFirstRow {
			isFirstRow = true
			continue
		}
		var rowStr string
		if row.Cells[0].String() == "" {
			break
		}
		for _, cell := range filter.Cells(row.Cells) {
			escapeStr := strings.Replace(cell.Value, `"`, `\"`, -1)
			rowStr += escapeStr + "\t"
			//rowStr += cell.String() + "\t"
		}
		exportFile.WriteString(rowStr + "\r\n")
	}
}

func import2db(db *sql.DB, exportFile *os.File, filename string) {
	execStr1 := fmt.Sprintf(`SET CHARACTER SET 'utf8'`)
	_, err := db.Exec(execStr1)
	if err != nil {
		panic(err.Error())
	}
	execStr2 := fmt.Sprintf("truncate %s", filename)
	_, err = db.Exec(execStr2)
	if err != nil {
		panic(err.Error())
	}
	filePath := strings.Replace(exportFile.Name(), `\\`, `/`, -1)
	mysql.RegisterLocalFile(filePath)
	sqlresult, err := db.Exec(`LOAD DATA LOCAL INFILE '` + filePath + `' INTO TABLE ` + filename + ` fields terminated by '\t' optionally enclosed by '"' lines terminated by '\r\n';`)
	if err != nil {
		panic(err.Error())
	}
	num, err := sqlresult.RowsAffected()
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("Import table [%s] end.Effected row num [%d]\n", filename, num)
	if num <= 0 {
		panic(errors.New("load file but have no effect"))
	}
}

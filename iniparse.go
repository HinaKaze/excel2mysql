package main

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

type Section struct {
	section string
	kvmap   map[string]string
}

var sectionMap map[string]*Section = make(map[string]*Section)

func GetSection(sName string) (section *Section, ok bool) {
	section, ok = sectionMap[sName]
	return
}

func (sec *Section) GetValue(key string) (value string, ok bool) {
	value, ok = sec.kvmap[key]
	return
}

func (sec *Section) GetIntValue(key string) (value int) {
	value, err := strconv.Atoi(sec.kvmap[key])
	if err != nil {
		panic(err.Error())
	}
	return
}

func DefaultParse(filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err.Error())
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	section := "default"
	for {
		bytes, _, err := reader.ReadLine()
		if err != nil || bytes == nil {
			break
		}
		if len(bytes) == 0 {
			continue
		}
		line := strings.TrimSpace(string(bytes))
		//ignor '//'
		if string(line[:2]) == "//" {
			continue
		}
		//load section
		if string(line[0]) == "[" && string(line[len(line)-1]) == "]" {
			section = line[1 : len(line)-1]
			if _, ok := sectionMap[section]; !ok {
				sectionMap[section] = &Section{section, make(map[string]string)}
			}
			continue
		}
		//ignor not key-value mode
		if strings.Index(line, "=") < 0 {
			continue
		}
		//load key-value
		key := line[:strings.Index(line, "=")]
		value := line[strings.Index(line, "=")+1:]
		sectionMap[section].kvmap[key] = value
	}
}

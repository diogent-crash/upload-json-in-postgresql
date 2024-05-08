package main

import (
	"archive/zip"
	"bufio"
	"log"
	"os"
	"os/exec"
	"strings"
)

func main() {
	// Открытие архива
	zipFile, err := zip.OpenReader("egrul.json.zip")
	if err != nil {
		log.Fatal(err)
	}
	defer zipFile.Close()

	// Создание базы
	cmd := exec.Command("psql", "-p", "5432", "-U", "postgres", "-c", "create database testdb;")
	_, err = cmd.Output()
	if err != nil {
		log.Fatal(err)
	}

	// Создание постоянной таблицы в базе
	cmd = exec.Command("psql", "-p", "5432", "-d", "testdb", "-U", "postgres", "-c", "create table if not exists egrul (ogrn varchar,inn varchar,kpp varchar,name varchar,short_name varchar);")
	_, err = cmd.Output()
	if err != nil {
		log.Fatal(err)
	}

	// Чтение и обработка каждого JSON-файла
	for _, file := range zipFile.File {
		// Открытие JSON-файла
		jsonFile, err := file.Open()
		if err != nil {
			log.Fatal(err)
		}

		// Чтение содержимого JSON-файла в память
		scanner := bufio.NewScanner(jsonFile)
		const maxCapacity int = 10240 * 10240 // your required line length
		buf := make([]byte, maxCapacity)
		scanner.Buffer(buf, maxCapacity)

		// Обработка JSON данных с помощью jq и удаление символов \t и \n с помощью sed
		for scanner.Scan() {
			line := scanner.Text()

			cmd := exec.Command("jq", "-cr", ".[]")
			cmd.Stdin = strings.NewReader(line)
			output, err := cmd.Output()
			if err != nil {
				log.Fatal(err)
			}

			line = string(output)

			cmd = exec.Command("sed", "s/\\[tn]//g; s/\\\\\"\\([^\\\\\"]*\\)\\\\\"/\\1/g")
			cmd.Stdin = strings.NewReader(line)
			output, err = cmd.Output()
			if err != nil {
				log.Fatal(err)
			}

			line = string(output)

			// Вставка данных во временную переменную
			tempData := line

			// Создание одного экземпляра exec.Command для подключения к базе данных
			dbCmd := exec.Command("psql", "-p", "5432", "-d", "testdb", "-U", "postgres")

			// Инициализация подключения к базе данных
			_, err = dbCmd.Output()
			if err != nil {
				log.Fatal(err)
			}

			// Создать временную таблицу
			cmd = exec.Command("psql", "-p", "5432", "-d", "testdb", "-U", "postgres", "-c", "create table if not exists temp (data varchar);")
			cmd.Stdin = strings.NewReader(tempData)
			cmd.Stdin = strings.NewReader(tempData)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err = cmd.Run()
			if err != nil {
				log.Fatal(err)
			}

			// Вставка данных в базу данных PostgreSQL
			cmd = exec.Command("psql", "-p", "5432", "-d", "testdb", "-U", "postgres", "-c", "COPY temp (data) FROM STDIN;")
			cmd.Stdin = strings.NewReader(tempData)
			cmd.Stdin = strings.NewReader(tempData)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err = cmd.Run()
			if err != nil {
				log.Fatal(err)
			}

			// Вставка выбранных данных в таблицу egrul
			insertQuery := `INSERT INTO egrul (ogrn, inn, kpp, name, short_name)
                SELECT
                    substring(data::TEXT FROM '"ogrn":"([^"]*)"') AS ogrn,
                    substring(data::TEXT FROM '"inn":"([^"]*)"') AS inn,
                    substring(data::TEXT FROM '"kpp":"([^"]*)"') AS kpp,
                    substring(data::TEXT FROM '"full_name":"([^"]*)"') AS name,
                    substring(data::TEXT FROM '"name":"([^"]*)"') AS short_name
                FROM temp;
`
			dbCmd = exec.Command("psql", "-p", "5432", "-d", "testdb", "-U", "postgres", "-c", insertQuery)
			dbCmd.Stdout = os.Stdout
			dbCmd.Stderr = os.Stderr
			err = dbCmd.Run()
			if err != nil {
				log.Fatal(err)
			}
			// Удаление временной таблицы
			cmd = exec.Command("psql", "-p", "5432", "-d", "testdb", "-U", "postgres", "-c", "drop table temp;")
			cmd.Stdin = strings.NewReader(tempData)
			cmd.Stdin = strings.NewReader(tempData)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err = cmd.Run()
			if err != nil {
				log.Fatal(err)
			}
		}

		if scanner.Err() != nil {
			log.Fatal(scanner.Err())
		}

		jsonFile.Close()
	}
}

package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/hellofresh/klepto/database"
)

// Dumper dumps a database's structure to a stream
type Dumper struct {
	conn *sql.DB
	out  chan []*database.Cell
	done chan bool
}

// Store provides an interface to access database stores.
type store interface {
	getTables() ([]string, error)
	getTableStructure() (string, error)
	getColumns() ([]string, error)
	getPreamble() (string, error)
}

// DumpStructurer provides an interface for implementing DumpStructure
type DumpStructurer interface {
	DumpStructure()
}

// NewDumper is the constructor for MySQLDumper
func NewDumper(conn *sql.DB) *Dumper {
	return &Dumper{
		conn: conn,
		out:  make(chan []*database.Cell, 1000),
		done: make(chan bool),
	}
}

// DumpStructure writes the database's structure to the provided stream
func (d *Dumper) DumpStructure() (structure string, err error) {
	preamble, err := d.getPreamble()
	if err != nil {
		return
	}

	tables, err := d.getTables()
	if err != nil {
		return
	}

	var tableStructure string
	for _, table := range tables {
		tableStructure, err = d.getTableStructure(table)
		if err != nil {
			return
		}
	}

	structure = fmt.Sprintf("%s\n%s;\n\n", preamble, tableStructure)
	return
}

// WaitGroupBufferer buffers table contents for each wait group.
func (d *Dumper) WaitGroupBufferer() []*bytes.Buffer {
	anonymiser := NewMySQLAnonymiser(d.conn)
	tables, err := d.getTables()
	if err != nil {
		color.Red("Error getting tables: %s", err.Error())
	}

	var (
		wg           sync.WaitGroup
		tableBuffers []*bytes.Buffer
	)

	wg.Add(len(tables))

	for _, table := range tables {
		columns, err := d.getColumns(table)
		buf := bytes.NewBufferString(fmt.Sprintf("\nINSERT INTO `%s` (%s) VALUES", table, strings.Join(columns, ", ")))

		go d.bufferer(buf, d.out, d.done, &wg)

		err = anonymiser.AnonymiseRows(table, d.out, d.done)
		if err != nil {
			color.Red("Error stealing data: %s", err.Error())
			return tableBuffers
		}

		b := buf.Bytes()
		b = b[:len(b)-1]
		b = append(b, []byte(";")...)
		tableBuffers = append(tableBuffers, buf)
	}

	close(d.out)
	wg.Wait()

	return tableBuffers
}

// getPreamble puts a big old comment at the top of the database dump.
// Also acts as first query to check for errors.
func (d *Dumper) getPreamble() (string, error) {
	preamble := `# *******************************
# This database was nicked by Klepto™.
#
# https://github.com/hellofresh/klepto
# Host: %s
# Database: %s
# Dumped at: %s
# *******************************

SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;

`
	var hostname string
	row := d.conn.QueryRow("SELECT @@hostname")
	err := row.Scan(&hostname)
	if err != nil {
		return "", err
	}

	var database string
	row = d.conn.QueryRow("SELECT DATABASE()")
	err = row.Scan(&database)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(preamble, hostname, database, time.Now().Format(time.RFC1123Z)), nil
}

// getTables gets a list of all tables in the database
func (d *Dumper) getTables() (tables []string, err error) {
	tables = make([]string, 0)
	var rows *sql.Rows
	if rows, err = d.conn.Query("SHOW FULL TABLES"); err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, tableType string
		if err = rows.Scan(&tableName, &tableType); err != nil {
			return
		}
		if tableType == "BASE TABLE" {
			tables = append(tables, tableName)
		}
	}
	return
}

// getColumns returns the columns in the specified database table
func (d *Dumper) getColumns(table string) (columns []string, err error) {
	var rows *sql.Rows
	if rows, err = d.conn.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table)); err != nil {
		return
	}
	defer rows.Close()
	if columns, err = rows.Columns(); err != nil {
		return
	}
	for k, column := range columns {
		columns[k] = fmt.Sprintf("`%s`", column)
	}
	return
}

// getTableStructure gets the CREATE TABLE statement of the specified database table
func (d *Dumper) getTableStructure(table string) (stmt string, err error) {
	row := d.conn.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table))
	var tableName string // We don't really care about this value but nevermind
	if err = row.Scan(&tableName, &stmt); err != nil {
		return "", err
	}

	return
}

func (d *Dumper) bufferer(buf *bytes.Buffer, rowChan chan []*database.Cell, done chan bool, wg *sync.WaitGroup) {
	for {
		select {
		case cells, more := <-rowChan:
			if !more {
				done <- true
				return
			}

			len := len(cells)
			for i, c := range cells {
				if i == 0 {
					buf.WriteString("\n(")
				}

				if c.Type == "string" {
					buf.WriteString(fmt.Sprintf("\"%s\"", c.Value))
				} else {
					buf.WriteString(fmt.Sprintf("%s", c.Value))
				}

				if i == len-1 {
					buf.WriteString("),")
				} else {
					buf.WriteString(", ")
				}
			}
		case <-done:
			wg.Done()
			return
		}
	}
}

package main

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
)

// Open database connection
func OpenConnection(conf ConnectionSetting, database string) (*sql.DB, error) {
	return sql.Open(conf.Driver, conf.DSN+database)
}

type TransferTask struct {
	Source  *sql.DB              // source database connector
	Target  *sql.DB              // target database connector
	Setting TableTransferSetting // Transfer Settings (with success)
	Success interface{}          // Concurrent success loaded
}

type ColumnDefinition struct {
	Name     string
	DataType string
	Nullable bool
}

// RunTransferTable
func RunTransferTables() {
	settings := GetConfigure(ConfigPath)
	success := GetSuccessor(settings.Successor)
	// save success at last
	// defer SaveToYaml(settings.Successor, success)

	// run each schema
	for schema, transfers := range settings.Targets {
		// open and close source
		source, _ := OpenConnection(settings.Connectors[KEY_CNX_SOURCE], schema)
		defer source.Close()
		// open and close target
		target, _ := OpenConnection(settings.Connectors[KEY_CNX_TARGET], schema)
		defer target.Close()

		if _, exists := success[schema]; !exists {
			success[schema] = make(map[string]interface{}, 0)
		}
		schemaSuccess := success[schema]

		fmt.Printf("DB %s\n", schema)

		for _, task := range transfers {
			// build transfer task
			sc, se := schemaSuccess[task.Name]
			if !se || sc == nil {
				sc = ""
			}
			fmt.Printf("  TABLE %s(%s)\n", task.Name, sc)
			tt := TransferTask{source, target, task, sc}
			// create the table if not exists
			tt.duplicateTable()
			// copy rows
			tt.copyRows()

			success[schema][task.Name] = tt.Success
		}
	}
	SaveToYaml(settings.Successor, success)
}

// RunTransferViews to duplicate views
func RunTransferViews() {
	settings := GetConfigure(ConfigPath)
	for schema, _ := range settings.Targets {
		// open and close source
		source, _ := OpenConnection(settings.Connectors[KEY_CNX_SOURCE], schema)
		defer source.Close()
		// open and close target
		target, _ := OpenConnection(settings.Connectors[KEY_CNX_TARGET], schema)
		defer target.Close()

		// duplicate views
		duplicateView(source, target, schema)

	}
}

// SyncTable duplicates table
func (tt *TransferTask) Sync() {
	// check target table exists
	tt.duplicateTable()

	// retrieve success lines
	tt.copyRows()
}

func scanRow(rss *sql.Rows, columns []string) []interface{} {
	values := make([]interface{}, len(columns))
	ptrs := make([]interface{}, len(columns))
	for i, _ := range values {
		ptrs[i] = &values[i]
	}

	rss.Scan(ptrs...)
	return values
}

func queryFetchAll(db *sql.DB, query string, args ...interface{}) ([][]interface{}, error) {
	rs, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	columns, _ := rs.Columns()
	rss := make([][]interface{}, 0)
	for rs.Next() {
		values := scanRow(rs, columns)
		rss = append(rss, values)
	}

	return rss, nil
}

func readTableColumns(db *sql.DB, table string, prefix string, build func([]interface{}) ColumnDefinition) []ColumnDefinition {
	cols, err := queryFetchAll(db, prefix+table)
	columns := make([]ColumnDefinition, len(cols))
	if err != nil {
		return nil
	}

	for i, col := range cols {
		columns[i] = build(col)
	}
	return columns
}

func buildMSSQLColumnDefinition(col []interface{}) ColumnDefinition {
	dataname := col[3].(string)
	// replace '%' char
	dataname = strings.ReplaceAll(dataname, "%", "%")
	datatype := col[5].(string)
	switch strings.ToLower(datatype) {
	case "numeric":
		if scale := col[8].(int64); 0 < scale {
			datatype = "double"
		} else if precision := col[6].(int64); 9 < precision {
			datatype = "bigint"
		} else {
			datatype = "int"
		}
	case "varchar":
		if datalen := col[7].(int64); datalen < 400 {
			datatype = fmt.Sprintf("varchar(%d)", datalen)
		} else {
			datatype = "text"
		}
	case "nvarchar":
		datatype = fmt.Sprintf("varchar(%d)", col[7].(int64))
	case "nchar":
		datatype = fmt.Sprintf("char(%d)", col[7].(int64))
	case "ntext":
		datatype = "text"
	case "datetime":
		datatype = "datetime"
	case "datetime2":
		datatype = "datetime"
	default:
		datatype = fmt.Sprintf("%s(%d)", datatype, col[7].(int64))
	}

	return ColumnDefinition{
		Name:     dataname,
		DataType: datatype,
		Nullable: 0 < col[10].(int64),
	}
}

func buildMySQLColumnDefinition(col []interface{}) ColumnDefinition {
	name := string(col[0].([]uint8))
	datatype := string(col[1].([]uint8))
	nullable := string(col[2].([]uint8)) != "YES"
	return ColumnDefinition{
		Name:     name,
		DataType: datatype,
		Nullable: nullable,
	}
}

//
func readMSSQLTableColumns(db *sql.DB, table string) []ColumnDefinition {
	return readTableColumns(db, table, "sp_columns ", buildMSSQLColumnDefinition)
}

func readMySQLTableColumns(db *sql.DB, table string) []ColumnDefinition {
	return readTableColumns(db, table, "SHOW COLUMNS FROM ", buildMySQLColumnDefinition)
}

func matchTableColumns(left []ColumnDefinition, right []ColumnDefinition) bool {
	return true
}

func buildMySQLTable(db *sql.DB, table string, columns []ColumnDefinition) {
	cols := make([]string, len(columns))

	for i, col := range columns {
		options := ""
		if !col.Nullable {
			options = "NOT NULL"
		}

		cols[i] = fmt.Sprintf("`%s` %s %s",
			strings.ReplaceAll(col.Name, "%", ""),
			col.DataType,
			options)
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (%s)",
		table, strings.Join(cols, ","))
	fmt.Printf(query)
	tx, _ := db.Begin()
	tx.Exec(query)
	defer tx.Commit()
}

func (tt TransferTask) duplicateTable() {
	// load ColumnDefinitions
	oldColumns := readMSSQLTableColumns(tt.Source, tt.Setting.Name)
	newColumns := readMySQLTableColumns(tt.Target, tt.Setting.Name)

	if len(newColumns) <= 0 {
		// has no table on target, build new
		buildMySQLTable(tt.Target, tt.Setting.Name, oldColumns)
	} else if !matchTableColumns(oldColumns, newColumns) {
		// TODO: alter the table
	}
}

type ReplacePattern struct {
	Pattern *regexp.Regexp
	Replace string
}

var replacePatterns = []ReplacePattern{
	{regexp.MustCompile(`(?i)create\sview`), "CREATE VIEW"},
	{regexp.MustCompile(`(?i)\[?dbo\]?\.`), ""},
	{regexp.MustCompile(`(?i)%`), ""},
	{regexp.MustCompile(`(?i)\[([^\]\[]+)\]`), "`$1`"},
}

func (rp ReplacePattern) ReplaceAll(s string) string {
	return rp.Pattern.ReplaceAllString(s, rp.Replace)
}

func copyViewQuery(old string) string {
	// viewname := args[0].(string)
	// definition := args[1].(string)
	query := old
	// retrieve patterns
	for _, rep := range replacePatterns {
		query = rep.ReplaceAll(query)
	}

	return query
}

func listMSSQLViews(source *sql.DB) map[string]string {
	query := `SELECT name, object_definition(object_id) FROM sys.views`
	rets := make(map[string]string)
	rss, err := source.Query(query)
	defer rss.Close()
	if err == nil {
		cols, _ := rss.Columns()
		for rss.Next() {
			row := scanRow(rss, cols)
			rets[row[0].(string)] = row[1].(string)
		}
	} else {
		fmt.Printf(err.Error())
	}
	// defer rss.Close()
	return rets
}

func listMySQLViews(target *sql.DB, db string) map[string]string {
	query := "SELECT TABLE_NAME, VIEW_DEFINITION from information_schema.views WHERE table_schema LIKE ?"
	rets := make(map[string]string)
	rss, err := target.Query(query, db)
	defer rss.Close()
	if err == nil {
		for rss.Next() {
			var name, def string
			// scan
			rss.Scan(&name, &def)
			//
			rets[name] = def
		}
	}

	return rets
}

// duplicate views
func duplicateView(source *sql.DB, target *sql.DB, db string) {
	// list source views
	oldViews := listMSSQLViews(source)
	newViews := listMySQLViews(target, db)

	for vname, def := range oldViews {
		if _, exists := newViews[vname]; !exists {
			if rs, err := target.Exec(copyViewQuery(def)); err != nil {
				fmt.Errorf(err.Error())
			} else {
				affected, _ := rs.RowsAffected()
				fmt.Printf("%d rows affected", affected)
			}
		}
	}
}

func (tt TransferTask) findSuccessColumnIndex(rss *sql.Rows, index string) int {
	columns, _ := rss.Columns()
	for i, col := range columns {
		if strings.ToLower(tt.Setting.Index) == strings.ToLower(col) {
			return i
		}
	}
	return -1
}

func (tt *TransferTask) copyRows() int {
	// FROM Latest success
	selects := fmt.Sprintf("SELECT * FROM [%s] WHERE @p1<[%s] ORDER BY [%s] ASC", tt.Setting.Name, tt.Setting.Index, tt.Setting.Index)
	// query success index
	rss, err := tt.Source.Query(selects, tt.Success)
	// pass
	if err != nil {
		return 0
	}
	columns, _ := rss.Columns()
	successIndex := tt.findSuccessColumnIndex(rss, tt.Setting.Index)
	latest := tt.Success

	// Transaction
	count := 0
	insertBase := "INSERT INTO `%s` VALUES (%s)"
	// build params string
	params := make([]string, len(columns))
	for i, _ := range params {
		params[i] = "?"
	}

	tx, _ := tt.Target.Begin()
	for rss.Next() {
		count += 1
		row := scanRow(rss, columns)
		inserts := fmt.Sprintf(insertBase, tt.Setting.Name, strings.Join(params, ","))
		tx.Exec(inserts, row...)

		// record latest index
		latest = row[successIndex]
		// writes for 20
		if count%20 == 0 {
			tx.Commit()
			// restart transaction
			tx, _ = tt.Target.Begin()
		}
	}
	// close read
	rss.Close()

	fmt.Printf("%d lines copied to %s\n", count, latest)

	// commit here
	if err := tx.Commit(); err == nil {
		// Success. update latest success record
		tt.Success = latest
	} else {
		fmt.Println(err.Error())
		// Rollback on Error
		tx.Rollback()
	}
	return count
}

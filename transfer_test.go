package main

import (
	"database/sql"
	"fmt"
	"testing"
)

func sampleTransferSettings() map[string][]TransferTask {
	conf := GetConfigure(ConfigPath)
	scs := GetSuccessor(conf.Successor)

	rets := make(map[string][]TransferTask)

	// return conf.Targets
	for schema, targets := range conf.Targets {
		source, _ := OpenConnection(conf.Connectors[KEY_CNX_SOURCE], schema)
		target, _ := OpenConnection(conf.Connectors[KEY_CNX_TARGET], schema)
		rets[schema] = make([]TransferTask, len(targets))
		for i, t := range targets {
			sc, exists := scs[schema][t.Name]
			if !exists {
				sc = ""
			}
			rets[schema][i] = TransferTask{
				Setting: t,
				Source:  source,
				Target:  target,
				Success: sc,
			}
		}
	}
	return rets
}

func testConnectorRaw(t *testing.T, driver string, dsn string) {
	t.Logf("RAW:: %s >> %s", driver, dsn)
	if cnx, err := sql.Open(driver, dsn); err == nil {
		defer cnx.Close()
		if err = cnx.Ping(); err != nil {
			t.Error("ping fail")
		}
	} else {
		t.Errorf("Connection failed: %s", err.Error())
	}
}

func testConnector(t *testing.T, config string) {
	settings := GetConfigure(ConfigPath)
	if conf, exists := settings.Connectors[config]; exists {
		cnx, err := OpenConnection(conf, "CheilOptimizer_DM")
		if err != nil {
			t.Errorf("connection Error: %s", err.Error())
		}
		defer cnx.Close()
		err = cnx.Ping()
		if err != nil {
			t.Errorf("ping error: %s", err.Error())
		}

	} else {
		t.Error("connector config not set")
	}
}

func TestConnectorRawLegacy(t *testing.T) {
	// pass
	// testConnectorRaw(t, "sqlserver", "")
}

func TestConnectorRawReplica(t *testing.T) {
	// pass
	// testConnectorRaw(t, "mysql", "")
}

// TestConnectSource - connecting to legacy (mssql) database
func TestConnectSource(t *testing.T) {
	testConnector(t, KEY_CNX_SOURCE)
}

// TestConnectTarget - connecting to new replica (mysql) database
func TestConnectTarget(t *testing.T) {
	testConnector(t, KEY_CNX_TARGET)
}

func TestQueryFetchAll(t *testing.T) {

}

func TestMSSQLColumnsRead(t *testing.T) {
	conf := GetConfigure(ConfigPath)
	// target, _ := OpenConnection(conf.Connectors[KEY_CNX_TARGET])

	for db, settings := range sampleTransferSettings() {
		source, _ := OpenConnection(conf.Connectors[KEY_CNX_SOURCE], db)
		defer source.Close()
		// target, _ := OpenConnection(conf.Connectors[KEY_CNX_TARGET], db)
		// defer target.Close()

		for _, task := range settings {
			task.Source = source
			// task.Target = target
			columns := readMSSQLTableColumns(source, task.Setting.Name)
			if len(columns) <= 0 {
				t.Error("can not find any column")
			}
			t.Logf("+ [TABLE] %s", task.Setting.Name)
			for i, c := range columns {
				t.Logf("  - %d.[%s] %s", i, c.DataType, c.Name)
			}
		}
	}
}

func TestMySQLColumnsRead(t *testing.T) {
	conf := GetConfigure(ConfigPath)

	for db, settings := range sampleTransferSettings() {
		// source, _ := OpenConnection(conf.Connectors[KEY_CNX_SOURCE])
		target, _ := OpenConnection(conf.Connectors[KEY_CNX_TARGET], db)
		defer target.Close()

		for _, task := range settings {
			columns := readMySQLTableColumns(target, task.Setting.Name)
			if len(columns) <= 0 {
				t.Error("can not find any column")
			}
			t.Logf("+ [TABLE] %s", task.Setting.Name)
			for i, c := range columns {
				t.Logf("  - %d.[%s] %s", i, c.DataType, c.Name)
			}
		}
	}
}

func TestDuplicateTable(t *testing.T) {

}

func TestBuildTable(t *testing.T) {
	db := `CheilOptimizer_DM`
	table := `tests`
	conf := GetConfigure(ConfigPath)
	target, _ := OpenConnection(conf.Connectors[KEY_CNX_TARGET], db)

	// drop table before run
	target.Exec("DROP TABLE IF EXISTS " + table)
	// reservation to clear out again
	// defer target.Exec("DROP TABLE IF EXISTS " + table)

	columns := []ColumnDefinition{
		{"id", "int", false},
		{"name", "varchar(50)", true},
		{"misc", "text", true},
	}

	buildMySQLTable(target, table, columns)

	// test columns
	cols := readMySQLTableColumns(target, table)
	if len(columns) != len(cols) {
		t.Error("table len not matched")
	}
}

func buildTaskForTest(db string, table string) TransferTask {
	conf := GetConfigure(ConfigPath)
	source, _ := OpenConnection(conf.Connectors[KEY_CNX_SOURCE], db)
	target, _ := OpenConnection(conf.Connectors[KEY_CNX_TARGET], db)
	tt := TransferTask{
		Source:  source,
		Target:  target,
		Setting: TableTransferSetting{Name: table, Index: "insert_dt"},
		Success: "",
	}
	return tt
}

func closeTask(tt TransferTask) {
	if err := tt.Source.Ping(); err == nil {
		tt.Source.Close()
	}
	if err := tt.Target.Ping(); err == nil {
		tt.Target.Close()
	}
}

func TestDuplicateTableColumns(t *testing.T) {
	tt := buildTaskForTest(`CheilOptimizer_DM`, `Cleansed_Dataset`)

	// before start, clear previous database
	tt.Target.Exec("DROP TABLE IF EXISTS " + tt.Setting.Name)

	tt.duplicateTable()
	columns := readMySQLTableColumns(tt.Target, tt.Setting.Name)
	if len(columns) <= 0 {
		t.Errorf("table not exists")
	}

	// close
	closeTask(tt)
}

func TestAlterTableColumns(t *testing.T) {
	// pass
}

func TestTableSuccessRead(t *testing.T) {
	tt := buildTaskForTest(`CheilOptimizer_DM`, `Cleansed_Dataset`)
	// read from source
	selects := fmt.Sprintf("SELECT * FROM %s WHERE @p1<%s ORDER BY %s ASC", tt.Setting.Name, tt.Setting.Index, tt.Setting.Index)

	// copyRow features
	var rss *sql.Rows
	var columns []string
	var err error
	if rss, err = tt.Source.Query(selects, tt.Success); err != nil {
		t.Errorf(err.Error())
	} else {
		t.Logf("(%s) %s ", selects, tt.Success)
	}
	if columns, err = rss.Columns(); err != nil {
		t.Errorf(err.Error())
	}
	successIndex := tt.findSuccessColumnIndex(rss, tt.Setting.Index)
	latest := tt.Success

	//
	if successIndex < 0 {
		t.Error("success index had not set")
	}

	t.Logf("concurrent latest == '%s'", latest)

	counts := 0

	for rss.Next() {
		counts += 1
		row := scanRow(rss, columns)
		t.Log(row...)
	}

	if counts <= 0 {
		t.Error("no records?")
	}

}

func TestTableSuccess(t *testing.T) {
	tt := buildTaskForTest(`CheilOptimizer_DM`, `Cleansed_Dataset`)
	// before start, clear previous database
	tt.Target.Exec("DROP TABLE IF EXISTS " + tt.Setting.Name)

	// table needed
	tt.duplicateTable()

	if 0 < len(tt.Success.(string)) {
		t.Error("success set before starts")
	}

	cnt := tt.copyRows()
	if cnt <= 0 {
		t.Error("data had not transferred")
	}

	t.Logf("final success: '%s'", tt.Success)

	if len(tt.Success.(string)) <= 0 {
		t.Error("success had not set")
	}

	closeTask(tt)
}

func TestListMSSQLViews(t *testing.T) {
	db := `CheilOptimizer_DM`
	conf := GetConfigure(ConfigPath)
	source, _ := OpenConnection(conf.Connectors[KEY_CNX_SOURCE], db)

	if views := listMSSQLViews(source); views == nil || len(views) <= 0 {
		t.Error(`No views specified`)
	} else {
		for name, def := range views {
			t.Logf("<%s> %s", name, def)
		}
	}
}

func TestListMySQLViews(t *testing.T) {
	db := `CheilOptimizer_DM`
	conf := GetConfigure(ConfigPath)
	target, _ := OpenConnection(conf.Connectors[KEY_CNX_TARGET], db)
	defer target.Close()

	if views := listMySQLViews(target, db); views == nil || len(views) <= 0 {
		t.Error(`No views specified`)
	} else {
		for name, def := range views {
			t.Logf("<%s> %s", name, def)
		}
	}
}

type PatternSample struct {
	Sample string
	Expect string
}

func TestReplacePatterns(t *testing.T) {

	samples := []PatternSample{
		{"create view", "CREATE VIEW IF NOT EXISTS"},
		{"CREATE view ", "CREATE VIEW IF NOT EXISTS "},
		{"[dbo].[cheil]", "`cheil`"},
		{"[a],[b],[c]", "`a`,`b`,`c`"},
	}

	for i, s := range samples {
		val := s.Sample
		t.Logf("[%d] %s ---", i, val)
		for j, p := range replacePatterns {
			val = p.ReplaceAll(val)
			t.Logf("  %d>> %s", j, val)
		}
		if s.Expect != val {
			t.Errorf(">>>expected %s but %s", s.Expect, val)
		}
	}
}

func TestCreateViewQuery(t *testing.T) {
	sample := `CREATE VIEW [dbo].[Angola_52_jinyeong.kim_cheil.com] AS
	SELECT  Date, [Campaign name], [Destination URL], [Segment name], [Creative name], [Publisher platform], [Planned cost_usd], [Planned cost_local], [Actual cost_usd], [Actual cost_local], [Planned Impressions], 
				   Impressions, [Viewable impression], [Planned CPM_usd], [Planned CPM_local], [Actual CPM_usd], [Actual CPM_local], [Planned Clicks], Clicks, [Planned CTR], CTR, [Planned CPC_usd], 
				   [Planned CPC_local], [Actual CPC_usd], [Actual CPC_local], [Planned Reach], Reach, Frequency, [Planned Views_video], Views_video, [Video watches at 25%], [Video watches at 50%], 
				   [Video watches at 75%], [Video watches at 100%], [Link clicks], [Post engagements], [Post comments], [Post shares], [Post Likes], CID, [CID 1], [CID 2], [CID 3], [CID 4], [CID 5], [CID 6], [CID 7], 
				   [CID 8], [CID 9], [CID 10], [CID 11], [CID 12], [CrawlMediaAccount], [CrawlMediaCode], [Planned CPV_views_usd], [Planned CPV_views_local], [Actual CPV_views_usd], 
				   [Actual CPV_views_local], [Post click conversions], [Post view conversions], [Total conversions], Follows, Retweets, Replies, [Planned VTR], VTR, [Engagement rate], Sent, Delivered, 
				   [Delivered rate], [Open], [Open rate], Display, [Display rate], [Clicks owned], CTR_delivered, CTR_display, CTOR, Campaign_name, Country, Campaign_start_dt, Campaign_end_dt, Cycle, 
				   Currency, Product, Advertiser, Description, [Week in campaign], Media_type, Media_name, Media_campaign, Confirm_user, confirm_date, reg_date, [Tracking code], [Planned visits], visits, 
				   [Planned C2V], C2V, [Planned CPV_visits_usd], [Planned CPV_visits_local], [Actual CPV_visits_usd], [Actual CPV_visits_local], [Unique visitors], [Page views], [Bounce rate], 
				   [Average time spent], [Planned Add to cart], [Add to cart], [Planned CVR], CVR, [Planned CPA_usd], [Planned CPA_local], [Actual CPA_usd], [Actual CPA_local], Checkout, Orders, Units, 
				   Revenue_usd, Revenue_local, ROAS_usd, ROAS_local
	FROM     [CheilOptimizer_DM].dbo.Cleansed_Dataset
	WHERE  (Campaign_name = '52' AND Country = 'AO' AND [DATE] IS NOT NULL)`
	changed := copyViewQuery(sample)
	t.Log(changed)

}

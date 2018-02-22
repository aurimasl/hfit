package main

import (
	"database/sql"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/go-fsnotify/fsnotify"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/ini.v1"
)

//
var watcher *fsnotify.Watcher

var dbs = make(map[string]uint64)

var (
	root = kingpin.Flag(
		"path",
		"Path to datadir.",
	).Default("/var/lib/mysql").Short('p').String()
	limit = kingpin.Flag(
		"limit", "DB limit in MB.",
	).Default("3072").Short('l').Uint64()
	configMycnf = kingpin.Flag(
		"config",
		"Path to .my.cnf file to read MySQL credentials from.",
	).Default(path.Join(os.Getenv("HOME"), ".my.cnf")).String()
	logFile = kingpin.Flag(
		"log",
		"Log file path",
	).String()
	dryRun = kingpin.Flag(
		"dry-run",
		"Do not revoke permissions",
	).Short('d').Bool()
	log *zap.SugaredLogger
	dsn string
)

func init() {
	kingpin.Parse()
	cfg := zap.NewProductionConfig()
	if *logFile != "" {
		cfg.OutputPaths = []string{*logFile}
	}

	logger, err := cfg.Build()
	if err != nil {
		panic("Could open log file.")
	}

	defer logger.Sync()
	log = logger.Sugar()
}

// main
func main() {
	*limit = *limit * uint64(1024*1024)
	log.Infof("Watching %s with limit %dMB", *root, *limit/1024/1024)
	var err error
	dsn, err = parseMycnf(*configMycnf)
	if err != nil {
		log.Fatal(err)
	}

	watcher, _ = fsnotify.NewWatcher()
	defer watcher.Close()

	if err := filepath.Walk(*root, watchDir); err != nil {
		log.Errorf("ERROR: %s", err)
	}

	done := make(chan bool)

	go func() {
		for {
			select {
			// watch for events
			case event := <-watcher.Events:
				log.Debugf("EVENT! %#v %s", event, event.String())

				if event.Op&fsnotify.Create == fsnotify.Create {
					f, err := os.Stat(event.Name)
					if err != nil {
						log.Errorf("Got err: %s", err)
					}
					if f.IsDir() {
						dirName := filepath.Base(event.Name)
						if isUserDb(dirName) {
							watcher.Add(event.Name)
							dbs[dirName], _ = dirSize(event.Name)
						}
					}
				} else if event.Op&fsnotify.Remove == fsnotify.Remove {

					_, exists := dbs[filepath.Base(event.Name)]
					if exists {
						delete(dbs, filepath.Base(event.Name))
					}
				} else if event.Op&fsnotify.Write == fsnotify.Write {
					f, e := os.Stat(event.Name)
					if e != nil {
						log.Errorf("Could not stat() on %s", event.Name)
					}
					dirName := filepath.Base(event.Name)

					// Recalculate direcotry size
					if !f.IsDir() {
						dirName = filepath.Base(filepath.Dir(event.Name))
					}
					if isUserDb(dirName) {
						dbs[dirName], _ = dirSize(filepath.Dir(event.Name))
					} else {
						log.Infof("Removing %s from watch list.", event.Name)
						watcher.Remove(event.Name)
					}
				} else if event.Op&fsnotify.Chmod == fsnotify.Chmod {
					// Don't even log
				} else {
					fmt.Println("Operation not handled.")
				}

				// watch for errors
			case err := <-watcher.Errors:
				log.Errorf("ERROR", "err", err)
			}
		}
	}()

	<-done
}

func watchDir(path string, fi os.FileInfo, err error) error {
	if fi.Mode().IsDir() {
		dirName := filepath.Base(path)
		if isUserDb(dirName) {
			dbs[dirName], _ = dirSize(path)
		}
		if isUserDb(dirName) || path == *root {
			log.Infof("Watching %s", path)
			return watcher.Add(path)
		}
	}
	return nil
}

func isUserDb(name string) bool {
	match, _ := regexp.MatchString(`^[a-z]\d{9}_.*`, name)
	return match
}

func dirSize(path string) (uint64, error) {
	var size uint64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += uint64(info.Size())
		}
		return err
	})
	log.Debugf("Size of %s is %d", path, size)

	if size >= *limit {
		log.Warnf(
			"%s exceeded limit of %dMB with it's %s. I must keep it fit.",
			filepath.Base(path),
			*limit/1024/1024,
			humanize.Bytes(size),
		)
		dbName := filepath.Base(path)
		if *dryRun != true {
			revokePermissions(dbName)
		}
	}
	return size, err
}

func revokePermissions(dbName string) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Infof("Error opening connection to database: %s", err)
		return
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(1 * time.Minute)

	_, err = db.Exec(
		fmt.Sprintf(
			"UPDATE mysql.db SET Insert_priv='N' WHERE Db = '%s'",
			dbName,
		),
	)
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic
	}
	_, err = db.Exec("FLUSH PRIVILEGES")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic
	}
	log.Infof("Permissions revoked on %s", dbName)

}

func parseMycnf(config interface{}) (string, error) {
	var dsn string
	cfg, err := ini.Load(config)
	if err != nil {
		return dsn, fmt.Errorf("failed reading ini file: %s", err)
	}
	user := cfg.Section("client").Key("user").String()
	password := cfg.Section("client").Key("password").String()
	if (user == "") || (password == "") {
		return dsn, fmt.Errorf("no user or password specified under [client] in %s", config)
	}
	host := cfg.Section("client").Key("host").MustString("localhost")
	port := cfg.Section("client").Key("port").MustUint(3306)
	socket := cfg.Section("client").Key("socket").String()
	if socket != "" {
		dsn = fmt.Sprintf("%s:%s@unix(%s)/", user, password, socket)
	} else {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port)
	}
	return dsn, nil
}

func checkWatch(path string) {
	f, err := os.Stat(path)
	if err != nil {
		log.Errorf("Got err:", err)
	}
	if !f.IsDir() {

	} else {
		dirName := filepath.Base(path)
		if isUserDb(dirName) {
			watcher.Add(path)
			dbs[dirName], _ = dirSize(path)
		}
	}
}

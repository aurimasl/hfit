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

type dbInfo struct {
	lastCheckAt time.Time
	size        uint64
}

var (
	datadir = kingpin.Flag(
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
	log     *zap.SugaredLogger
	dsn     string
	watcher *fsnotify.Watcher

	dbs = make(map[string]dbInfo)
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

func main() {
	*limit = *limit * uint64(1024*1024)
	*datadir += "/*"
	log.Infof("Starting watch on %s with limit %s", *datadir, humanize.IBytes(*limit))
	var err error
	dsn, err = parseMycnf(*configMycnf)
	if err != nil {
		log.Fatal(err)
	}

	watcher, _ = fsnotify.NewWatcher()
	defer watcher.Close()

	done := make(chan bool)
	c1 := make(chan string)

	go func() {
		for {
			c1 <- *datadir
			time.Sleep(time.Minute)
		}
	}()

	go func() {
		for {
			select {
			// watch for events
			case event := <-watcher.Events:
				log.Debugf("EVENT! %#v %s", event, event.String())
				now := time.Now()

				if event.Op&fsnotify.Create == fsnotify.Create {
					dirName := filepath.Base(event.Name)
					if isUserDb(dirName) {
						watcher.Add(event.Name)
					}
				} else if event.Op&fsnotify.Remove == fsnotify.Remove {

					_, exists := dbs[filepath.Base(event.Name)]
					if exists {
						log.Infof("Removing %s from watch list.", filepath.Base(event.Name))
						delete(dbs, filepath.Base(event.Name))
					}
				} else if event.Op&fsnotify.Write == fsnotify.Write {
					f, e := os.Stat(event.Name)
					if e != nil {
						// Most likely tmp table was removed
						continue
					}

					dirName := filepath.Base(event.Name)
					if !f.IsDir() {
						dirName = filepath.Base(filepath.Dir(event.Name))
					}

					if isUserDb(dirName) {
						if time.Since(dbs[dirName].lastCheckAt) > 5*time.Second {
							size := dirSize(filepath.Dir(event.Name))
							if size > *limit {
								if *dryRun != true {
									revokePermissions(dirName)
								}
							}
							dbs[dirName] = dbInfo{size: size, lastCheckAt: now}
						}
					} else {
						log.Infof("Removing %s from watch list.", event.Name)
						err = watcher.Remove(event.Name)
						if err != nil {
							log.Errorf("Could not remove from watch list: %s", err)
						}
					}

				} else if event.Op&fsnotify.Chmod == fsnotify.Chmod {
					// Don't even log
				} else if event.Op&fsnotify.Rename == fsnotify.Rename {
					// Don't even log
				} else {
					log.Infof("Unhandled event! %#v %s", event, event.String())
				}

				// watch for errors
			case err := <-watcher.Errors:
				log.Errorf("ERROR", "err", err)
			case <-c1:
				scanDatadir()
			}

		}
	}()

	<-done
}

func scanDatadir() {
	log.Infof("Scanning datadir: %s", *datadir)
	entries, _ := filepath.Glob(*datadir)
	for _, entry := range entries {
		dirName := filepath.Base(entry)
		_, exists := dbs[dirName]
		if exists {
			continue
		}
		if isUserDb(dirName) {
			log.Debugf("Watching %s", dirName)
			watcher.Add(entry)
			dbs[dirName] = dbInfo{}
		}
	}
	log.Infof("Databases watching: %d", len(dbs))
}

func watchDir(path string, fi os.FileInfo, err error) error {
	if fi.Mode().IsDir() {
		dirName := filepath.Base(path)
		_, exists := dbs[dirName]
		if exists {
			return nil
		} else {
			if isUserDb(dirName) {
				return watcher.Add(path)
			}
		}
	}
	return nil
}

func isUserDb(name string) bool {
	match, _ := regexp.MatchString(`^[a-z]\d{9}_.*`, name)
	return match
}

func dirSize(path string) uint64 {
	var size uint64
	size = 0
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			log.Infof("Failure accessing a path %q: %v", path, err)
			return err
		}
		if !info.IsDir() {
			size += uint64(info.Size())
			return nil
		}
		return err
	})
	log.Debugf("Size of %s is %d", path, size)

	if size >= *limit {
		log.Warnf(
			"%s exceeded limit of %s with it's %s. I must keep it fit.",
			filepath.Base(path),
			humanize.IBytes(*limit),
			humanize.IBytes(size),
		)
	}
	return size
}

func revokePermissions(dbName string) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Infof("Error opening connection to database: %s", err)
		return
	}
	defer db.Close()
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(1 * time.Minute)

	stmt, err := db.Prepare(
		fmt.Sprintf(
			"SELECT User, Host FROM mysql.db WHERE Db = '%s' AND (Insert_priv = 'Y' OR Update_priv = 'Y')",
			dbName,
		),
	)
	if err != nil {
		panic(err.Error())
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		panic(err.Error())
	}
	stmt.Close()
	defer rows.Close()
	for rows.Next() {
		var user string
		var host string
		if err := rows.Scan(&user, &host); err != nil {
			panic(err.Error())
		}
		log.Infof("REVOKE INSERT, UPDATE ON %s.* FROM '%s'@'%s'", dbName, user, host)
		_, err = db.Exec(
			fmt.Sprintf(
				"REVOKE INSERT, UPDATE ON %s.* FROM '%s'@'%s'",
				dbName,
				user,
				host,
			),
		)
		if err != nil {
			panic(err.Error())
		}
		err = rows.Err()
		if err != nil {
			panic(err)
		}
	}
	log.Infof("Permissions revoked on %s", dbName)
	return
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

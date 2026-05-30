package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	dbengine "github.com/rodrigo0345/omag/src/engine"
	"github.com/rodrigo0345/omag/src/query/server"
	applog "github.com/rodrigo0345/omag/pkg/pkglog"
)

func main() {
	listenAddr := flag.String("listen", ":5432", "TCP address to listen on for psql connections")
	dbPath := flag.String("db", "./omag.db", "path to the database file")
	lsmDataDir := flag.String("lsm-data-dir", "./lsm_data", "directory for LSM table data")
	walPath := flag.String("wal", "./omag.wal", "path to the WAL file")
	pprofListen := flag.String("pprof-listen", "", "optional pprof listen address, e.g. :6060")
	debug := flag.Bool("debug", false, "enable debug logs")
	flag.Parse()

	if *debug {
		applog.SetLevel(applog.LevelDebug)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	applog.Info("[OMAG] starting pgwire server listen=%s db=%s lsm=%s wal=%s debug=%v",
		*listenAddr, *dbPath, *lsmDataDir, *walPath, *debug)

	db, err := dbengine.OpenMVCCLSM(dbengine.Options{
		DBPath:     *dbPath,
		LSMDataDir: *lsmDataDir,
		WALPath:    *walPath,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to open database:", err)
		os.Exit(1)
	}
	defer func() {
		if err := db.Close(); err != nil {
			fmt.Fprintln(os.Stderr, "close error:", err)
		}
	}()

	if strings.TrimSpace(*pprofListen) != "" {
		pprofServer := &http.Server{Addr: strings.TrimSpace(*pprofListen), Handler: http.DefaultServeMux}
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = pprofServer.Shutdown(shutdownCtx)
		}()
		go func() {
			if err := pprofServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Fprintln(os.Stderr, "pprof server error:", err)
			}
		}()
	}

	srv := server.New(db)
	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to listen:", err)
		os.Exit(1)
	}

	if err := srv.Serve(ctx, ln); err != nil && err != context.Canceled {
		fmt.Fprintln(os.Stderr, "server error:", err)
		os.Exit(1)
	}
}

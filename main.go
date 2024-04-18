package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"

	"context"
	"fmt"
	"os"
	"path/filepath"
	"plugin"
	"sync"
	"time"

	"github.com/rs/xid"
	"github.com/turbot/tailpipe-plugin-sdk/collection"
	"github.com/turbot/tailpipe-plugin-sdk/observer"
	"github.com/turbot/tailpipe-plugin-sdk/source"
	"github.com/turbot/tailpipe/tailpipe"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func playSource() {
	// Load the plugin
	plug, err := plugin.Open("../tailpipe-source-file/tailpipe-source-file.so")
	if err != nil {
		panic(err)
	}

	// Look up the exported symbol (Plugin)
	symPlugin, err := plug.Lookup("Plugin")
	if err != nil {
		panic(err)
	}

	// Assert that loaded symbol is of Plugin interface type
	var p source.Plugin
	p, ok := symPlugin.(source.Plugin)
	if !ok {
		panic("Plugin has no 'Plugin' type")
	}

	// Example configuration in JSON format
	//configJSON := []byte(`{"path":"/Users/nathan/src/play-duckdb/daily-enriched", "extensions": [".parquet"]}`)
	configJSON := []byte(`{"path":"/Users/nathan/src/play-duckdb/2023/01/15", "extensions": [".gz"]}`)

	// Load the plugin-specific config
	if err := p.LoadConfig(configJSON); err != nil {
		panic(err)
	}

	if err := p.ValidateConfig(); err != nil {
		panic(err)
	}

	ctx := context.Background()
	s, _ := tailpipe.NewSource(ctx, p.Identifier(), "test")

	done := make(chan struct{})
	o := tailpipe.NewObserver(func(ei observer.Event) {
		// switch based on the struct of the event
		switch e := ei.(type) {
		case *tailpipe.EventArtifactDiscovered:
			fmt.Printf("Discovered : %s\n", e.ArtifactInfo.Name)
		case *tailpipe.EventDownloadArtifactStart:
			fmt.Printf("Downloading: %s\n", e.ArtifactInfo.Name)
		case *tailpipe.EventDownloadArtifactProgress:
			fmt.Printf("Done  %2d/%2d: %s\n", e.Current, e.Total, e.ArtifactInfo.Name)
		case *tailpipe.EventArtifactDownloaded:
			fmt.Printf("Received   : %s\n", e.Artifact.Name)
		case *tailpipe.EventDownloadArtifactEnd:
			fmt.Printf("Downloaded : %s\n", e.ArtifactInfo.Name)
		case *tailpipe.EventDownloadEnd:
			close(done)
		}
	}).Start()

	s.AddObserver(o)

	err = s.Download()
	if err != nil {
		panic(err)
	}

	select {
	case <-done:
		fmt.Println("Download complete")
	case <-time.After(5 * time.Second):
		fmt.Println("Download timed out")
	}

}

func playCollection() {
	// Load the plugin
	plug, err := plugin.Open("../tailpipe-collection-aws/tailpipe-collection-aws.so")
	if err != nil {
		panic(err)
	}

	// Look up the exported symbol (Plugin)
	symPlugin, err := plug.Lookup("Plugin")
	if err != nil {
		panic(err)
	}

	// Assert that loaded symbol is of Plugin interface type
	var p collection.Plugin
	p, ok := symPlugin.(collection.Plugin)
	if !ok {
		panic("Plugin has no 'Plugin' type")
	}

	// Example configuration in JSON format
	configJSON := []byte(`{"path":"/Users/nathan/src/play-duckdb/daily-enriched", "extensions": [".parquet"]}`)

	// Load the plugin-specific config
	if err := p.LoadConfig(configJSON); err != nil {
		panic(err)
	}

	if err := p.ValidateConfig(); err != nil {
		panic(err)
	}

	ctx := context.Background()
	c, _ := tailpipe.NewCollection(ctx, "aws_cloudtrail", "test", "file")
	p.AddObserver(c)

	// If it doesn't exist, create a new one.
	// TODO - make this secure and safe
	//outputFile := fmt.Sprintf("%s/%s/%s.parquet", outputDir, hivePath, xid.New().String())
	outputFile := fmt.Sprintf("/Users/nathan/Downloads/test_%s.parquet", xid.New().String())

	// Create the directory if it doesn't exist
	err = os.MkdirAll(filepath.Dir(outputFile), 0755)
	if err != nil {
		panic(err)
	}

	fw, err := local.NewLocalFileWriter(outputFile)
	if err != nil {
		panic(err)
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, c.GetPlugin().Schema(), 4)
	if err != nil {
		panic(err)
	}

	// See https://github.com/apache/parquet-format?tab=readme-ov-file#configurations
	pw.RowGroupSize = 64 * 1024 * 1024
	pw.PageSize = 256 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	fmt.Println("Create new writer: ", outputFile, pw.CompressionType.String(), pw.PageSize, pw.RowGroupSize)

	done := make(chan struct{})
	o := tailpipe.NewObserver(func(ei observer.Event) {
		// switch based on the struct of the event
		switch e := ei.(type) {
		case *tailpipe.EventRow:
			if err := pw.Write(e.Row); err != nil {
				fmt.Printf("Failed to write: %v\n", e.Row)
			}
			/*
				fmt.Printf("Row: %v\n", e.Row)
				ba, err := json.Marshal(e.Row)
				if err != nil {
					panic(err)
				}
				fmt.Println("output", string(ba))
			*/
		case *tailpipe.EventExtractRowsEnd:
			close(done)
		}
	}).Start()

	c.AddObserver(o)

	err = c.ExtractArtifactRows(&source.Artifact{ArtifactInfo: source.ArtifactInfo{Name: "test"}})
	if err != nil {
		panic(err)
	}

	select {
	case <-done:
		fmt.Println("Extraction complete")
	case <-time.After(5 * time.Second):
		fmt.Println("Extraction timed out")
	}

	if err := pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
	}

	//c.Play()
}

func awsCloudTrailLocalSource() *tailpipe.Source {

	/*

		// Load the plugin
		plug, err := plugin.Open("../tailpipe-source-file/tailpipe-source-file.so")
		if err != nil {
			panic(err)
		}

		// Look up the exported symbol (Plugin)
		symPlugin, err := plug.Lookup("Plugin")
		if err != nil {
			panic(err)
		}

		// Assert that loaded symbol is of Plugin interface type
		var p source.Plugin
		p, ok := symPlugin.(source.Plugin)
		if !ok {
			panic("Plugin has no 'Plugin' type")
		}

	*/

	p := tailpipe.GetConfig().SourcePlugin("file")

	// Example configuration in JSON format
	configJSON := []byte(`{"path":"/Users/nathan/src/play-duckdb/2023/01", "extensions": [".gz"]}`)

	// Load the plugin-specific config
	if err := p.LoadConfig(configJSON); err != nil {
		panic(err)
	}

	if err := p.ValidateConfig(); err != nil {
		panic(err)
	}

	ctx := context.Background()
	pluginIdentifier := p.Identifier()
	s, err := tailpipe.NewSource(ctx, pluginIdentifier, pluginIdentifier)
	if err != nil {
		panic(err)
	}

	return s
}

func awsCloudTrailLocalCollection(s *tailpipe.Source) *tailpipe.Collection {

	/*

		// Load the plugin
		plug, err := plugin.Open("../tailpipe-collection-aws/tailpipe-collection-aws.so")
		if err != nil {
			panic(err)
		}

		// Look up the exported symbol (Plugin)
		symPlugin, err := plug.Lookup("Plugin")
		if err != nil {
			panic(err)
		}

		// Assert that loaded symbol is of Plugin interface type
		var p collection.Plugin
		p, ok := symPlugin.(collection.Plugin)
		if !ok {
			panic("Plugin has no 'Plugin' type")
		}

	*/

	p := tailpipe.GetConfig().CollectionPlugin("aws")

	// Example configuration in JSON format
	configJSON := []byte(`{"name": "my_aws", "plugin": "aws", "source": "file"}`)

	// Load the plugin-specific config
	if err := p.LoadConfig(configJSON); err != nil {
		panic(err)
	}

	if err := p.ValidateConfig(); err != nil {
		panic(err)
	}

	ctx := context.Background()
	pluginIdentifier := p.Identifier()
	c, err := tailpipe.NewCollection(ctx, pluginIdentifier, "test", s.Name)
	p.AddObserver(c)
	if err != nil {
		panic(err)
	}

	return c
}

func playCollection2() {

	s := tailpipe.GetConfig().Source("file")
	c := tailpipe.GetConfig().Collection("aws")
	//s := awsCloudTrailLocalSource()
	//c := awsCloudTrailLocalCollection(s)

	var wg sync.WaitGroup

	extractCount := 0
	writeCount := 0

	done := make(chan struct{})
	o := tailpipe.NewObserver(func(ei observer.Event) {
		// switch based on the struct of the event
		switch e := ei.(type) {
		/*
			case *tailpipe.EventArtifactDownloaded:
				downloadCount++
				fmt.Println("EventArtifactDownloaded", e.Artifact.Name, downloadCount)
				err := c.ExtractArtifactRows(e.Artifact)
				if err != nil {
					panic(err)
				}
		*/
		case *tailpipe.EventRow:
			//fmt.Printf("EventRow: %s\n", e.Artifact.Name)
			wg.Add(1)
			writeCount++
			go func() {
				defer wg.Done()
				//fmt.Printf("+")
				err := c.WriteRow(e.Row)
				//fmt.Printf("#")
				if err != nil {
					panic(err)
				}
				writeCount--
				fmt.Println("Row burndown: ", writeCount)
			}()
		case *tailpipe.EventExtractRowsStart:
			extractCount++
			fmt.Println("EventExtractRowsStart:", e.Artifact.Name, extractCount)
			wg.Add(1)
		case *tailpipe.EventExtractRowsEnd:
			extractCount--
			fmt.Println("EventExtractRowsEnd:", e.Artifact.Name, e.Error, extractCount)
			wg.Done()
		case *tailpipe.EventDownloadStart:
			fmt.Println("EventDownloadStart")
		case *tailpipe.EventDiscoverArtifactsStart:
			fmt.Println("EventDiscoverArtifactsStart")
		case *tailpipe.EventArtifactDiscovered:
			fmt.Printf("EventArtifactDiscovered: %s\n", e.ArtifactInfo.Name)
			a := &source.Artifact{ArtifactInfo: *e.ArtifactInfo}
			go func() {
				err := c.ExtractArtifactRows(a)
				if err != nil {
					panic(err)
				}
			}()
		case *tailpipe.EventDiscoverArtifactsEnd:
			fmt.Println("EventDiscoverArtifactsEnd", e.Error)
			wg.Done()
		case *tailpipe.EventDownloadEnd:
			fmt.Println("EventDownloadEnd", e.Error)
		}
	}).Start()

	s.AddObserver(o)
	c.AddObserver(o)

	wg.Add(1)
	err := c.DiscoverArtifacts()
	//err := c.Download()
	if err != nil {
		panic(err)
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("Extraction complete")
	case <-time.After(600 * time.Second):
		fmt.Println("Extraction timed out")
	}

	fmt.Println("CloseWriters")
	if err := c.CloseWriters(); err != nil {
		fmt.Println("Close error", err)
	}

}

func playCollectionWithSync() {

	s := tailpipe.GetConfig().Source("my_pipes")
	c := tailpipe.GetConfig().Collection("my_pipes_log")

	var wg sync.WaitGroup

	o := tailpipe.NewObserver(func(ei observer.Event) {
		switch e := ei.(type) {
		case *tailpipe.EventArtifactDiscovered:
			//fmt.Printf("EventArtifactDiscovered: %s\n", e.ArtifactInfo.Name)
			a := &source.Artifact{ArtifactInfo: *e.ArtifactInfo}
			wg.Add(1)
			go func() {
				err := c.SyncArtifactRows(a)
				if err != nil {
					panic(err)
				}
			}()
		case *tailpipe.EventSyncArtifactStart:
			//fmt.Println("EventSyncArtifactStart", e.Artifact.Name)
		case *tailpipe.EventSyncArtifactEnd:
			//fmt.Println("EventSyncArtifactEnd", e.Artifact.Name)
			wg.Done()
		case *tailpipe.EventDiscoverArtifactsEnd:
			//fmt.Println("EventDiscoverArtifactsEnd", e.Error)
			wg.Done()
		}
	}).Start()

	s.AddObserver(o)
	c.AddObserver(o)

	wg.Add(1)
	err := c.DiscoverArtifacts()
	if err != nil {
		panic(err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("Extraction complete")
	case <-time.After(6000 * time.Second):
		fmt.Println("Extraction timed out")
	}

	fmt.Println("CloseWriters")
	if err := c.CloseWriters(); err != nil {
		fmt.Println("Close error", err)
	}

}

func main() {

	// Start the pprof server in a separate goroutine
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	if _, err := tailpipe.InitConfig(context.Background()); err != nil {
		panic(err)
	}
	if err := tailpipe.GetConfig().LoadPlugins(); err != nil {
		panic(err)
	}
	cfgJSON := `{
		"sources": {
			"my_file": {
				"name": "my_file",
				"plugin": "file",
				"config": {
					"path": "/Users/nathan/src/play-duckdb/2023/02",
					"extensions": []
				}
			}
		},
		"collections": {
			"my_aws_log": {
				"plugin": "aws_cloudtrail_log",
				"name": "my_aws_log",
				"source": "my_file"
			}
		}
	}`
	cfgJSON = `{
		"sources": {
			"my_pipes": {
				"name": "my_pipes",
				"plugin": "pipes_audit_log"
			}
		},
		"collections": {
			"my_pipes_log": {
				"plugin": "pipes_audit_log",
				"name": "my_pipes_log",
				"source": "my_pipes"
			}
		}
	}`
	if err := tailpipe.GetConfig().LoadBytes([]byte(cfgJSON)); err != nil {
		panic(err)
	}

	//playSource()
	//playCollection()
	//playCollection2()
	playCollectionWithSync()
}

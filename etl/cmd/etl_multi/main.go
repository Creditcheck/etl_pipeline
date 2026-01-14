package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"etl/internal/multitable"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "", "path to multi-table pipeline config JSON")
	flag.Parse()

	if cfgPath == "" {
		fmt.Fprintln(os.Stderr, "usage: etl_multi -config path/to/pipeline.json")
		os.Exit(2)
	}

	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read config: %v\n", err)
		os.Exit(1)
	}

	var cfg multitable.Pipeline
	if err := json.Unmarshal(raw, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "parse config: %v\n", err)
		os.Exit(1)
	}

	r := multitable.NewDefaultRunner()
	if err := r.Run(context.Background(), cfg); err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("ok")
}

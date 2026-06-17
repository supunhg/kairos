package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	v1 "github.com/supunhg/kairos/api/v1"
	"github.com/supunhg/kairos/internal/persistence"
	"github.com/supunhg/kairos/internal/sync"
)

func main() {
	dir := flag.String("dir", "./data", "Persistence directory")
	after := flag.String("after", "", "Replay from this event ID (exclusive)")
	limit := flag.Int("limit", 0, "Max events to replay (0 = all)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	eng, err := persistence.Open(*dir, persistence.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open persistence: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = eng.Close() }()

	evCount, snapCount := eng.Stats()
	fmt.Fprintf(os.Stderr, "Events: %d, Snapshots: %d\n", evCount, snapCount)

	engine := sync.NewEngine("replay")
	processed := 0

	cb := func(ev *v1.Event) error {
		if err := engine.Apply(ctx, []*v1.Event{ev}); err != nil {
			return fmt.Errorf("apply %s: %w", ev.Id, err)
		}
		processed++
		if *limit > 0 && processed >= *limit {
			return fmt.Errorf("reached limit")
		}
		return nil
	}

	var replayErr error
	if *after != "" {
		replayErr = eng.ReplayFrom(ctx, *after, cb)
	} else {
		replayErr = eng.Replay(ctx, cb)
	}
	if replayErr != nil && replayErr.Error() != "reached limit" {
		fmt.Fprintf(os.Stderr, "replay error: %v\n", replayErr)
	}

	fmt.Printf("Replay complete: %d events processed\n", processed)

	for _, groupID := range engine.GroupIDs() {
		fmt.Printf("Group: %s\n", groupID)
		content := engine.TextContent(groupID)
		if content != "" {
			fmt.Printf("  Content: %q\n", content)
		}
		vv := engine.GetVersionVector(groupID)
		for node, ts := range vv {
			fmt.Printf("  %s: %d\n", node, ts)
		}
		fmt.Println()
	}
}

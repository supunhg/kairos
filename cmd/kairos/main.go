package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/supunhg/kairos/internal/identity"
	kairos "github.com/supunhg/kairos/pkg/sdk"
)

func main() {
	nodeID := flag.String("node", "default", "Node ID")
	addr := flag.String("addr", ":8443", "Listen address")
	peer := flag.String("peer", "", "Peer address to connect to")
	identityPath := flag.String("identity", identity.DefaultIdentityPath(), "Ed25519 identity key file")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	ident, err := identity.LoadIdentityFile(*identityPath)
	if err != nil {
		ident, err = identity.Generate()
		if err != nil {
			fmt.Fprintf(os.Stderr, "generate identity: %v\n", err)
			os.Exit(1)
		}
		if err := identity.SaveIdentityFile(*identityPath, ident); err != nil {
			fmt.Fprintf(os.Stderr, "save identity: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Generated new identity: %s\n", ident.ID())
	}

	client := kairos.New(*nodeID, kairos.WithIdentity(ident))

	if *peer != "" {
		if err := client.Connect(ctx, *peer); err != nil {
			fmt.Fprintf(os.Stderr, "connect: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Connected to %s\n", *peer)
	}

	sess, err := client.Join(ctx, "cli-session")
	if err != nil {
		fmt.Fprintf(os.Stderr, "join: %v\n", err)
		os.Exit(1)
	}

	doc, err := sess.Document(ctx, "readme")
	if err != nil {
		fmt.Fprintf(os.Stderr, "document: %v\n", err)
		os.Exit(1)
	}

	unsub := doc.Subscribe(ctx, func(ev *kairos.Event) {
		text := doc.Text(ctx)
		fmt.Printf("\n--- Event ---\n")
		fmt.Printf("Type: %s\n", ev.PayloadType)
		fmt.Printf("Content: %s\n", text)
		fmt.Printf("-------------\n")
	})
	defer unsub()

	doc.Insert(ctx, 0, "Hello from "+*nodeID+"!")

	fmt.Printf("KAIROS node '%s' running on %s\n", *nodeID, *addr)
	fmt.Println("Press Ctrl+C to exit")

	select {
	case <-sig:
		fmt.Println("\nShutting down...")
	case <-ctx.Done():
	}

	cancel()
	client.Close()
}

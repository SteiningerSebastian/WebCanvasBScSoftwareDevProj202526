package main

import (
	"context"
	"fmt"
	pixelhasher "general/pixel_hasher"
	"time"
	veritasclient "veritas-client"
)

func main() {
	hashA := pixelhasher.PixelToKey(123, 456)
	hashB := pixelhasher.PixelToKey(124, 456)
	hashC := pixelhasher.PixelToKey(123, 457)
	fmt.Printf("Pixel hash A: %d\n", hashA)
	fmt.Printf("Pixel hash B: %d\n", hashB)
	fmt.Printf("Pixel hash C: %d\n", hashC)
	fmt.Println("Hello, Partitioning Controller!")

	// Try the veritas client
	client, err := veritasclient.NewVeritasClient([]string{"localhost:8080"}, 200*time.Millisecond, 3, 2, 5*time.Second)
	if err != nil {
		fmt.Printf("Error creating Veritas client: %v\n", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Second)
	defer cancel()
	value, err := client.GetVariable(ctx, "service-noredb")
	if err != nil {
		fmt.Printf("Error getting variable: %v\n", err)
		return
	}
	fmt.Printf("Value of variable 'service-noredb': %s\n", value)

	ctx, cancel = context.WithTimeout(context.Background(), 500*time.Second)
	defer cancel()
	// Watch a variable
	updateChan, errorChan, err := client.WatchVariables(ctx, []string{"service-partitioning-controller-001"})
	if err != nil {
		fmt.Printf("Error watching variables: %v\n", err)
		return
	}

	go func() {
		for {
			select {
			case update, ok := <-updateChan:
				if !ok {
					fmt.Println("Update channel closed.")
					cancel()
					return
				}
				fmt.Printf("Received update: key=%s, value=%s\n", update.Key, update.NewValue)
			case err, ok := <-errorChan:
				if !ok {
					fmt.Println("Error channel closed.")
					cancel()
					return
				}
				fmt.Printf("Error while watching: %v\n", err)
			}
		}
	}()

	ctx, cancel = context.WithTimeout(context.Background(), 500*time.Second)
	defer cancel()
	// Load service registrations
	handler, err := veritasclient.NewServiceRegistrationHandler(ctx, client, "service-noredb")
	if err != nil {
		fmt.Printf("Error creating service registration handler: %v\n", err)
		return
	}

	handler.AddListener(func(sr veritasclient.ServiceRegistration) {
		fmt.Printf("New Service Registration update: %s\n", sr.ID)
		fmt.Printf("Endpoints:\n")
		for _, ep := range sr.Endpoints {
			fmt.Printf(" - ID: %s, Address: %s, Port: %d, Timestamp: %s\n", ep.ID, ep.Address, ep.Port, ep.Timestamp.String())
		}
		fmt.Printf("Meta:\n")
		for k, v := range sr.Meta {
			fmt.Printf(" - %s: %s\n", k, v)
		}
	})

	// Keep the main function alive for a while to receive updates
	time.Sleep(60 * time.Second)

}

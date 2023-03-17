package main

import (
	"context"
	_ "embed"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"

	"google.golang.org/api/chat/v1"
	"google.golang.org/api/option"
)

//go:embed aqueous-helper-380804-4802de4c497b.json
var keypath string

func fileContent(location string) map[string][]string {
	// Open the CSV file
	file, err := os.Open(location)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Parse the CSV file
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	// Create a map to store the senders and receivers
	senderMap := make(map[string][]string)

	// Iterate over the records and add the senders and receivers to the map
	for _, record := range records {
		sender := record[0]
		receivers := record[1:]
		if _, ok := senderMap[sender]; !ok {
			senderMap[sender] = []string{}
		}
		for _, receiver := range receivers {
			senderMap[sender] = append(senderMap[sender], strings.TrimSpace(receiver))
		}
	}

	// return the map
	return senderMap
}

func chatSend(actualMap map[string][]string) {
	message := &chat.Message{
		Text: "hello",
	}
	ctx := context.Background()
	client, err := chat.NewService(ctx, option.WithCredentialsJSON([]byte(keypath)))
	if err != nil {
		log.Fatalf("Unable to create chat client: %v", err)
	}
	log.Printf("hello client : %v\n", client.BasePath)

	for sender, recipients := range actualMap {
		for _, recipient := range recipients {
			_, err = client.Spaces.Messages.Create("im/"+recipient, message).ThreadKey(sender).Do()
			if err != nil {
				log.Fatalf("%v", err)
			}
			log.Printf("Msgs sent successfully from %s to %s\n", sender, recipient)
		}
	}
	log.Println("all the msgs sent properly")
}

func fileExsistance(location string) bool {
	filePath := location

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return false
	} else {
		return true
	}

}

func main() {
	var location string
	fmt.Println("Please Enter the Location of the data file: ")
	fmt.Scanln(&location)
	a := fileExsistance(location)
	if a {
		actualMap := fileContent(location)

		fmt.Println(actualMap)
		chatSend(actualMap)

	} else {
		fmt.Println("File does not exists please provide valid location")
		return
	}

}

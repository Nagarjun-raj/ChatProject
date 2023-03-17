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

func senderByReceiverEmailIds(location string) (map[string][]string, error) {
	// Open the CSV file
	file, err := os.Open(location)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Parse the CSV file
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
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
	return senderMap, nil
}

func sendChatMsgs(actualMap map[string][]string) error {
	message := &chat.Message{
		Text: "hello",
	}
	ctx := context.Background()
	client, err := chat.NewService(ctx, option.WithCredentialsJSON([]byte(keypath)))
	if err != nil {
		return err
	}

	for sender, recipients := range actualMap {
		for _, recipient := range recipients {
			_, err = client.Spaces.Messages.Create("im/"+recipient, message).ThreadKey(sender).Do()
			if err != nil {
				log.Printf("error : %v , while sending msgs from %s to %s\n", err, sender, recipient)
			} else {
				log.Printf("Msgs sent successfully from %s to %s\n", sender, recipient)
			}
		}
	}
	return nil
}

func isFileLocationValid(filePath string) bool {
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return false
	}
	return true
}

func main() {
	var filePath string
	fmt.Println("Please Enter the Path of the Data File: ")
	fmt.Scanln(&filePath)

	if !isFileLocationValid(filePath) {
		log.Println("File does not exists please provide valid File Path")
		return
	}

	senderToRecipientsEmailIds, err := senderByReceiverEmailIds(filePath)
	if err != nil {
		log.Printf("error while reading or opening file : %v\n ", err)
	}
	if len(senderToRecipientsEmailIds) == 0 {
		log.Println("Please check the data in the file, there doesnt seem to be any data")
	}
	err = sendChatMsgs(senderToRecipientsEmailIds)
	if err != nil {
		log.Printf("error while creating chat client object : %v\n", err)
	}

}

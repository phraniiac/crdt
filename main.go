package main

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/phraniiac/crdt/crdt"
	"os"
	"strconv"
	"strings"
	"time"
)

func createServer() *crdt.Server {
	server := crdt.CreateServer()
	return &server
}

func createClient() *crdt.Client {
	id, _ := uuid.NewUUID()

	client := crdt.GetClient(id.String())
	return client
}

func createClientsForApp(num int, app_id string, server *crdt.Server) map[int]*crdt.Client {

	clients := make(map[int]*crdt.Client)

	for i := 0; i < num; i++ {
		c := createClient()
		clients[i] = c

		c.WorkOnApplication(app_id, server)
	}
	return clients
}


func main() {

	server := createServer()

	// For Application 1. Can be repeated for any number of applications.
	// The server object can remain same.
	numClients := 4
	client1 := createClient()
	appId := client1.CreateAnApplication(server)
	clients := createClientsForApp(numClients, appId, server)
	clients[numClients] = client1

	runManualSimulation(clients, appId, server)
	runTests(clients, appId, server)
}


/**
	This routine helps the developer manually enter payload.
	If no client is specified, we direct the payload to any available client.
	Syntax - k,v,op,client_idx
	op = '0' -> add, '1' -> remove, '2' -> update
	client_idx: int = (0, len(clients)
	will break once -1,-1,-1,-1 is encoountered.
 */
func runManualSimulation(clients map[int]*crdt.Client, appId string, server *crdt.Server) {
	// run forever until user issue bye
	for {
		consoleReader := bufio.NewReader(os.Stdin)
		fmt.Printf("Client Array - [0, %d]: >", len(clients) - 1)

		input, _ := consoleReader.ReadString('\n')

		inputs := strings.Split(input, ",")

		if len(inputs) == 4 {

			for i := range inputs {
				inputs[i] = strings.TrimSpace(inputs[i])
			}

			inp, _ := strconv.Atoi(inputs[0])
			if inputs[0] == inputs[1] && inputs[2] == inputs[3] && inputs[1] == inputs[2] && inp == -1 {
				fmt.Println("Good bye!")
				os.Exit(0)
			} else {
				key, value, operator, clientIdx := inputs[0], inputs[1], inputs[2], inputs[3]

				op, _ := strconv.Atoi(operator)
				clientIndex, _ := strconv.Atoi(clientIdx)

				if clientIndex > len(clients) - 1 {
					clientIndex = 0
				}
				client := clients[clientIndex]

				value = value + crdt.SEPARATOR + strconv.FormatInt(time.Now().Unix(), 10)

				payload := crdt.CreatePayload(key, value, op, client.Id)
				go clients[clientIndex].MutateApplicationState(payload)
			}
		}

		if strings.TrimSpace(input) == "check_state" {
			// print and verify states of all the clients
			// and also of the application.

			fmt.Printf("\n\n\n\nCheck Status\n\n")
			for client := range clients {
				fmt.Println("ClientId:"+clients[client].Id+"; Data: ")
				fmt.Println(clients[client].GetCurrentState())
			}

			// Now we check if the maps are equal.
			//fmt.Println(checkMapsEqual(clients))
		}

		if len(inputs) != 4 {
			fmt.Println("Please enter proper input. (key, value, op, client).")
		}

		if strings.HasPrefix(input, "bye") {
			fmt.Println("Good bye!")
			os.Exit(0)
		}
	}
}

func checkMapsEqual(client map[int]*crdt.Client) bool {
	referenceIndex := 0
	referenceMap := client[referenceIndex].GetCurrentState()
	res := true

	for i := 1; i < len(client); i++ {
		for k, v := range client[i].GetCurrentState() {
			if v != referenceMap[k] {
				res = false
				return res
			}
		}
	}
	return res
}

func runTests(clients map[int]*crdt.Client, appId string, server *crdt.Server) {

}


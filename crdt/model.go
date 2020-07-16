package crdt

import "fmt"

/**
	The idea is that a set of clients can work on an application.
	They will communicate with each other using channels.
	Each of the client will have a Element Set.

	To create a new application, we need a client.



	The application will also have an ElementSet.
	Any new client who joins the application, will get the current_state
	from Application's ElementSet.
	The application will backup the ElementSet once,
	all the clients have stopped working on it.
	And hence the new initial state will be updated.
	And the operation history will be cleared, since backup will be saved.

 */

type OP int

const (
	ADD = iota
	REMOVE
	UPDATE
)

type Payload struct {
	clientId	string
	op 			int
	key 		string
	val 		string
}

func CreatePayload(key string, val string, op int, clientId string) Payload {
	return Payload{
		clientId: clientId,
		op:       op,
		key:      key,
		val:      val,
	}
}

type Client struct {
	// Can be MAC Address.
	Id 						string

	applicationId			string

	elementSet 				*LWWElementSet

	// Updated will be listened on this channel.
	listenerChannel			chan Payload

	// Updates made on the client will be pushed on this channel.
	pushUpdateChannel		chan Payload
}

func GetClient(id string) *Client {
	return &Client{
		Id:                id,
		applicationId:     "",
		elementSet:        CreateLWWElementSet(),
		listenerChannel:   make(chan Payload),
		pushUpdateChannel: nil,
	}
}

// Entrypoint - 1
func (client *Client) CreateAnApplication(server *Server) string {
	applicationId := server.CreateAndRegisterApplication()
	client.applicationId = applicationId
	client.WorkOnApplication(client.applicationId, server)
	return applicationId
}

// Entrypoint - 2
func (client *Client) WorkOnApplication (applicationId string, server *Server) {

	// This method will register the listener channel of this client
	// to the application's push updates channel. Also this will get the initial
	// app data. This is oversimplification of somewhat like a WebRTC protocol or rest API pub-sub system.

	// In reality there will be an actual rest API or message queues between the two object,
	// not just goland channels, as these objects will exists on different machines.


	initialData, applicationUpdateChannel := server.GetRequestedApplication(client.Id,
		client.listenerChannel,
		applicationId)

	client.elementSet.InitialSet = initialData

	// Attach the updates push channel to the object.
	client.pushUpdateChannel = applicationUpdateChannel

	// Unblockingly Start listening to updates from the application.
	go client.ListenForUpdates()
}


func (client *Client) ListenForUpdates() {

	fmt.Printf("Starting listening to application channel; For client: %s\n", client.Id)
	for payload := range client.listenerChannel {
		// Only apply this if the payload was not sent by this client itself.
		if payload.clientId != client.Id {
			fmt.Printf("Received payload in state: %s, for client: %s\n",
				payload.key+":"+payload.val+"_"+string(payload.op), client.Id)
			switch payload.op {
			case ADD:
				client.elementSet.UpsertElement(payload)
				break
			case REMOVE:
				client.elementSet.RemoveElement(payload)
				break
			case UPDATE:
				client.elementSet.UpsertElement(payload)
				break
			}
		}
	}
}

func (client *Client) MutateApplicationState (payload Payload) {
	fmt.Printf("Triggering a mutation in state: %s, for client: %s\n\n",
				payload.key+":"+payload.val+"_"+string(payload.op), payload.clientId)

	// Apply the change locally.
	switch payload.op {
		case ADD:
			client.elementSet.UpsertElement(payload)
			break
		case REMOVE:
			client.elementSet.RemoveElement(payload)
			break
		case UPDATE:
			client.elementSet.UpsertElement(payload)
			break
	}

	client.pushUpdateChannel <- payload
}

func (client *Client) GetCurrentState() map[string]string {
	return client.elementSet.ViewElements()
}

func (client *Client) CloseWorkOnApplication (server Server) {
	server.CloseApplicationForClient(client.Id, client.applicationId)
}

type Application struct {

	Id 				string

	// In application, we will store only latest and final copy.
	// This will be periodically copied to disk/persistent storage as a backup mechanism.
	// This will be also used as the initial data for any new clients.
	data			map[string]string

	lww 			*LWWElementSet

	// --> Operational Data.
	// Current active sessions for this application.
	// key is client ID and value is the client's listener channel.
	activeSessions 	map[string]chan Payload

	// Payload updates related to this application
	// are pushed and read in this channel.
	updates			chan Payload
}


func (*Application) BackupApplication() {

}

func (app *Application) ListenToPayloads() {

	for payload := range app.updates {

		fmt.Println("From Channel:" + payload.clientId)

		// Apply this update on Application's data.
		switch payload.op {
		case ADD:
			app.lww.UpsertElement(payload)
			break
		case REMOVE:
			app.lww.RemoveElement(payload)
			break
		case UPDATE:
			app.lww.UpsertElement(payload)
			break
		}

		// Send this update to all the subscribed channels.
		for _, listener := range app.activeSessions {
			listener <- payload
		}
	}
}

func (app *Application) GetInitialData() map[string]string {
	// We update the current data using application's lww
	app.data = app.lww.ViewElements()

	// We can also commit (persist) this to disk if we want to.
	return app.data
}

func (app *Application) AddListenerClient(clientId string, clientListener chan Payload) (map[string]string, chan Payload) {
	// We directly overwrite the clientLister if the clientId exists.
	// This incorporates if the client restarted or so.
	app.activeSessions[clientId] = clientListener

	// We return the initial data and the channel on which client has to push updates to.
	return app.data, app.updates
}

func (app *Application) CloseApplicationForClient(clientId string) {

	delete(app.activeSessions, clientId)

	// We can check if there are no more clients, then we can choose to write data on persistent
	// and delete the application object from server memory.
}

/**
	Only one object of this exists for the application.
	Can be horizontally scaled with a load balancer.
 */
type Server struct {
	// Application ID -> Application Pointer map.
	applicationLists		map[string]*Application

}

func CreateServer() Server {
	return Server{applicationLists: make(map[string]*Application)}
}

func (server *Server) CreateAndRegisterApplication() string {
	app_id := "test-id"
	app := &Application{
		Id:             app_id,
		data:           make(map[string]string),
		lww: 			CreateLWWElementSet(),
		activeSessions: make(map[string]chan Payload),
		updates:        make(chan Payload),
	}

	server.applicationLists[app_id] = app

	// start listening to updates channel.
	go app.ListenToPayloads()

	return app.Id
}

func (server *Server) GetRequestedApplication(clientId string,
												clientListener chan Payload,
												appId string) (map[string]string, chan Payload) {

	// TODO: Handle if no application.
	app := server.applicationLists[appId]
	return app.AddListenerClient(clientId, clientListener)
}

func (server *Server) CloseApplicationForClient(clientId string, appId string) {
	app := server.applicationLists[appId]
	app.CloseApplicationForClient(clientId)
}
package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var mu sync.Mutex
var rooms = make(map[string][]*websocket.Conn)

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	roomId := r.URL.Query().Get("roomId")

	if roomId == "" {
		http.Error(w, "Room Id required", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	mu.Lock()
	rooms[roomId] = append(rooms[roomId], conn)
	mu.Unlock()

	log.Println("User connected to room:", roomId)

	for {
		var msg map[string]string
		err := conn.ReadJSON(&msg)
		if err != nil {
			log.Println("Read error:", err)
			break
		}
		broadcast(roomId, msg)
	}

	removeConnection(roomId, conn)
	conn.Close()

}

func broadcast(roomId string, msg map[string]string) {
	mu.Lock()
	conns := rooms[roomId]
	mu.Unlock()

	for _, c := range conns {
		err := c.WriteJSON(&msg)
		if err != nil {
			log.Println("Write error:", err)
		}
	}
}

func removeConnection(roomId string, conn *websocket.Conn) {
	mu.Lock()
	defer mu.Unlock()
	conns := rooms[roomId]
	for i, c := range conns {
		if c == conn {
			rooms[roomId] = append(conns[:i], conns[i+1:]...)
			break
		}
	}
}

func main() {
	http.HandleFunc("/ws", handleWebSocket)

	log.Println("Server running on :8080")
	http.ListenAndServe(":8080", nil)
}

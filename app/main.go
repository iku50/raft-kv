package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"raft-kv/client"

	"github.com/gorilla/mux"
)

var _cli *client.Client

type GetResponse struct {
	Value string `json:"value"`
}

type PutRequest struct {
	Value string `json:"value"`
}

type PutResponse struct {
	Success bool `json:"success"`
}

type DeleteResponse struct {
	Success bool `json:"success"`
}

func main() {
	// TODO: add parse args
	// TODO: add config
	lb := client.NewMap(3, nil)
	_cli = client.NewClient(
		client.WithClusters("region", "country"),
		client.WithLoadBalance(&lb),
	)
	_cli.Start()
	stopCh := make(chan struct{})
	defer close(stopCh)
	go func() {
		<-stopCh
		_cli.Stop()
	}()
	r := mux.NewRouter()
	r.HandleFunc("/{key}", get).Methods(http.MethodGet)
	r.HandleFunc("/{key}", put).Methods(http.MethodPost)
	r.HandleFunc("/{key}", del).Methods(http.MethodDelete)
	if err := http.ListenAndServe(":8080", r); err != nil {
		return
	}
}

func del(writer http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key, ok := vars["key"]
	if !ok {
		_, _ = fmt.Fprintf(writer, "no key")
		return
	}
	err := _cli.Delete(key)
	if err != nil {
		log.Println(err)
		return
	}
	resp := DeleteResponse{}
	resp.Success = true
	_ = json.NewEncoder(writer).Encode(&resp)
	return
}

func put(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key, ok := vars["key"]
	if !ok {
		_, _ = fmt.Fprintf(w, "no key")
		return
	}
	var req PutRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	value := req.Value
	err := _cli.Put(key, value)
	if err != nil {
		log.Println(err)
		return
	}
	resp := PutResponse{}
	resp.Success = true
	_ = json.NewEncoder(w).Encode(&resp)
	return
}

func get(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key, ok := vars["key"]
	if !ok {
		_, _ = fmt.Fprintf(w, "no key")
		return
	}
	value, err := _cli.Get(key)
	if err != nil {
		log.Println(err)
		return
	}
	resp := GetResponse{}
	resp.Value = value
	_ = json.NewEncoder(w).Encode(&resp)
	return
}

package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

func NewHTTPServer(addr string) *http.server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()
	r.handleFunc("/" httpsrv.handleProduce).Methods("POST")
	r.handleFunc("/" httpsrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr: addr,
		Handler: r,
	}
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumerRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumerResponse struct {
	Record Record `json:"record"`
}

(s *httpServer) handleProduce(w http.ResonseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.body).Decode(&req)
	if err != nil {
		http.error(w, err.Error(), http.StatusBadRequest)
		return
	}
	off, err := s.Log.Append(req.Record)
	if err != nil {
		http.error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ProduceResponse{Offset: off}
	err = json.ENcoder(w).Encode(res)
	if err != nil {
		http.error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

(s *httpServer) handleConsume(w http.ResonseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.body).Decode(&req)
	if err != nil {
		http.error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ConsumeResponse{Record: record}
	err = json.Encoder(w).Encode(res)
	if err != nil {
		http.error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}



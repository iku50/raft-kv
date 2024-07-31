package main

import (
	"encoding/json"
	"log"
	"log/slog"
	"net/http"
	"raft-kv/client"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
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
		client.WithClusters("1"),
		client.WithLoadBalance(&lb),
	)
	_cli.Start()
	stopCh := make(chan struct{})
	defer close(stopCh)
	go func() {
		for {
			select {
			case <-stopCh:
				_cli.Stop()
			}
			time.After(500 * time.Millisecond)
		}
	}()
	r := fiber.New()
	r.Use(func(c fiber.Ctx) error {
		// gen a request id
		uuid := uuid.New()
		c.Request().Header.Set("X-Request-Id", uuid.String())
		// log request
		slog.Info("Request", "requestId", uuid, "method", c.Method(), "path", c.Path())
		err := c.Next()
		if err != nil {
			// log error
			slog.Error("Error", "requestId", uuid, "error", err.Error(), "method", c.Method(), "path", c.Path(), "body", string(c.Body()))
			return err
		}
		// log response
		slog.Info("Response", "requestId", uuid, "method", c.Method(), "path", c.Path())
		return nil
	})
	r.Get("/:key", get)
	r.Post("/:key", put)
	r.Delete("/:key", del)
	if err := r.Listen(":8080"); err != nil {
		slog.Error(err.Error())
		panic(err)
	}
}

func del(c fiber.Ctx) error {
	key := c.Params("key")
	if key == "" {
		return c.SendStatus(http.StatusBadRequest)
	}
	err := _cli.Delete(key)
	if err != nil {
		slog.Error(err.Error())
		return err
	}
	resp := DeleteResponse{Success: true}
	return c.JSON(resp)
}

func put(ctx fiber.Ctx) error {
	key := ctx.Params("key")
	if key == "" {
		return ctx.SendStatus(http.StatusBadRequest)
	}
	var req PutRequest
	err := json.Unmarshal(ctx.Body(), &req)
	if err != nil {
		return err
	}
	if req.Value == "" {
		return ctx.SendStatus(http.StatusBadRequest)
	}
	err = _cli.Put(key, req.Value)
	if err != nil {
		log.Println(err)
		return err
	}
	resp := PutResponse{Success: true}
	return ctx.JSON(resp)
}

func get(ctx fiber.Ctx) error {
	key := ctx.Params("key")
	if key == "" {
		return ctx.SendStatus(http.StatusBadRequest)
	}
	value, err := _cli.Get(key)
	if err != nil {
		log.Println(err)
		return err
	}
	resp := GetResponse{Value: value}
	return ctx.JSON(resp)
}

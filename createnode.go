package main

import (
	"fmt"
	"log"

	"github.com/neo4j/neo4j-go-driver/neo4j"
)

func main() {
	fmt.Println("Eventing Graph Data - Neo4j & RabbitMQ")
	result, err := helloWorld("bolt://localhost:7687", "neo4j", "password")
	if err != nil {
		log.Fatalf("%s, Error retrieving the result from graph", err)
	}
	log.Println("Node Created With ID :", result)
}

func helloWorld(uri, username, password string) (string, error) {
	var (
		err      error
		driver   neo4j.Driver
		session  neo4j.Session
		result   neo4j.Result
		greeting interface{}
	)
	driver, err = neo4j.NewDriver(uri, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		return "", err
	}
	defer driver.Close()

	session, err = driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return "", err
	}
	defer session.Close()

	greeting, err = session.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
		result, err = transaction.Run(
			"CREATE (a:DB) SET a.id = {id}, a.name = {name}, a.type = {type} RETURN a.id",
			map[string]interface{}{"id": "0", "name": "Reddis", "type": "Cache"})
		if err != nil {
			return nil, err
		}

		if result.Next() {
			return result.Record().GetByIndex(0), nil
		}

		return nil, result.Err()
	})
	if err != nil {
		return "", err
	}
	return greeting.(string), nil
}

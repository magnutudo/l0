package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"github.com/nats-io/stan.go"
	"log"
	"net/http"
	"time"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "Emil2001!"
	dbname   = "WB"
)

type jsonStructure struct {
	OrderUid    string `json:"order_uid"`
	TrackNumber string `json:"track_number"`
	Entry       string `json:"entry"`
	Delivery    struct {
		Name    string `json:"name"`
		Phone   string `json:"phone"`
		Zip     string `json:"zip"`
		City    string `json:"city"`
		Address string `json:"address"`
		Region  string `json:"region"`
		Email   string `json:"email"`
	} `json:"delivery"`
	Payment struct {
		Transaction  string `json:"transaction"`
		RequestId    string `json:"request_id"`
		Currency     string `json:"currency"`
		Provider     string `json:"provider"`
		Amount       int    `json:"amount"`
		PaymentDt    int    `json:"payment_dt"`
		Bank         string `json:"bank"`
		DeliveryCost int    `json:"delivery_cost"`
		GoodsTotal   int    `json:"goods_total"`
		CustomFee    int    `json:"custom_fee"`
	} `json:"payment"`
	Items []struct {
		ChrtId      int    `json:"chrt_id"`
		TrackNumber string `json:"track_number"`
		Price       int    `json:"price"`
		Rid         string `json:"rid"`
		Name        string `json:"name"`
		Sale        int    `json:"sale"`
		Size        string `json:"size"`
		TotalPrice  int    `json:"total_price"`
		NmId        int    `json:"nm_id"`
		Brand       string `json:"brand"`
		Status      int    `json:"status"`
	} `json:"items"`
	Locale            string    `json:"locale"`
	InternalSignature string    `json:"internal_signature"`
	CustomerId        string    `json:"customer_id"`
	DeliveryService   string    `json:"delivery_service"`
	Shardkey          string    `json:"shardkey"`
	SmId              int       `json:"sm_id"`
	DateCreated       time.Time `json:"date_created"`
	OofShard          string    `json:"oof_shard"`
}

var Cache = make(map[string]jsonStructure, 50)

func getById(c *gin.Context) {
	id := c.Param("id")
	result, ok := Cache[id]
	if ok {
		res, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return
		}
		c.HTML(http.StatusOK, "index.html", gin.H{
			"info": string(res),
		})
		return
	}
	c.IndentedJSON(http.StatusNotFound, gin.H{
		"message": "no info",
	})
}
func main() {
	jsonItem := (`{
		"order_uid": "b563feb7b2b84b6test",
		"track_number": "WBILMTESTTRACK",
		"entry": "WBIL",
		"delivery": {
		  "name": "Test Testov",
		  "phone": "+9720000000",
		  "zip": "2639809",
		  "city": "Kiryat Mozkin",
		  "address": "Ploshad Mira 15",
		  "region": "Kraiot",
		  "email": "test@gmail.com"
		},
		"payment": {
		  "transaction": "b563feb7b2b84b6test",
		  "request_id": "",
		  "currency": "USD",
		  "provider": "wbpay",
		  "amount": 1817,
		  "payment_dt": 1637907727,
		  "bank": "alpha",
		  "delivery_cost": 1500,
		  "goods_total": 317,
		  "custom_fee": 0
		},
		"items": [
		  {
			"chrt_id": 9934930,
			"track_number": "WBILMTESTTRACK",
			"price": 453,
			"rid": "ab4219087a764ae0btest",
			"name": "Mascaras",
			"sale": 30,
			"size": "0",
			"total_price": 317,
			"nm_id": 2389212,
			"brand": "Vivienne Sabo",
			"status": 202
		  }
		],
		"locale": "en",
		"internal_signature": "",
		"customer_id": "test",
		"delivery_service": "meest",
		"shardkey": "9",
		"sm_id": 99,
		"date_created": "2021-11-26T06:22:19Z",
		"oof_shard": "1"
	  }`)
	sqlConfig := fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", sqlConfig)
	if err != nil {
		log.Fatal(err)
	}
	rows, err := db.Query("SELECT * FROM wb_data")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var dbId string
		var dbJson string
		var jsnData = jsonStructure{}
		err := rows.Scan(&dbId, &dbJson)
		if err != nil {
			log.Fatal(err)
		}
		err = json.Unmarshal([]byte(dbJson), &jsnData)
		if err != nil {
			log.Fatal(err)
		}
		Cache[dbId] = jsnData
	}
	defer db.Close()

	sc, _ := stan.Connect("test-cluster", "myID")
	err = sc.Publish("boba", []byte(jsonItem))
	if err != nil {
		log.Fatalf("Unable to publish %v", err)
	}

	_, _ = sc.QueueSubscribe("boba", "bar", func(m *stan.Msg) {
		subData := jsonStructure{}
		err := json.Unmarshal(m.Data, &subData)
		if err != nil {
			log.Fatalf("Unable to Unmarshal")
		}
		fmt.Println(subData)
		subId := subData.OrderUid
		Cache[subId] = subData
		if err != nil {
			panic(err)
		}

		_, err = db.Exec("insert into wb_data (data) values ($1)", m.Data)
		if err != nil {
			log.Fatalf("Unable to insert")
		}
	}, stan.StartWithLastReceived())

	r := gin.Default()
	r.LoadHTMLGlob("templates/*.html")
	
	r.GET("api/:id", getById)

	s := &http.Server{
		Addr:           ":8080",
		Handler:        r,
		MaxHeaderBytes: 1 << 20,
	}
	s.ListenAndServe()

	fmt.Println("Server is listening...")
}

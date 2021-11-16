package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Movie struct {
	Id    string `json:"id"`
	Title string `json:"title"`
	Rank  string `json:"rank"`
}

var (
	db  *sql.DB
	err error
)

func main() {
	start := time.Now()
	dsn := "root:hacktiv@tcp(127.0.0.1:3306)/go_rest_api?charset=utf8mb4&parseTime=True&loc=Local"
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}
	log.Println("Database connected")

	movies, err := getMovieData()
	if err != nil {
		panic(err)
	}

	jobs := make(chan Movie, 0)
	wg := new(sync.WaitGroup)

	go workerDispattcher(db, jobs, wg)
	DistributeJobToWorker(movies, jobs, wg)
	wg.Wait()

	duration := time.Since(start)
	fmt.Println("done in", int(math.Ceil(duration.Seconds())), "seconds")

}

func workerDispattcher(db *sql.DB, jobs <-chan Movie, wg *sync.WaitGroup) {
	totalWorker := 10
	for i := 0; i < totalWorker; i++ {
		go func(worker int, db *sql.DB, jobs <-chan Movie, wg *sync.WaitGroup) {
			for job := range jobs {
				Job(worker, db, job)
				wg.Done()
			}

		}(i, db, jobs, wg)
	}
}

func Job(worker int, db *sql.DB, data Movie) {
	for {
		var outerError error

		func(outerError *error) {
			defer func() {
				if err := recover(); err != nil {
					*outerError = fmt.Errorf("%v", err)
				}
			}()

			conn, err := db.Conn(context.Background())
			if err != nil {
				log.Fatal(err.Error())
			}
			query := "INSERT INTO movie (id, title, `rank`) VALUES (?,?,?)"
			_, err = conn.ExecContext(context.Background(), query, data.Id, data.Title, data.Rank)
			if err != nil {
				log.Fatal(err.Error())
			}

			err = conn.Close()
			if err != nil {
				log.Fatal(err.Error())
			}
			log.Printf("worker %v success insert movie with id: %v", worker, data.Id)

		}(&outerError)

		if outerError == nil {
			break
		}
	}
}

func DistributeJobToWorker(movies []Movie, jobs chan<- Movie, wg *sync.WaitGroup) {
	for _, movie := range movies {
		wg.Add(1)
		jobs <- movie
	}
	close(jobs)
}

func getMovieData() ([]Movie, error) {
	filePath := "data/movies-100.json"
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var movies []Movie
	err = json.Unmarshal(data, &movies)
	if err != nil {
		return nil, err
	}
	return movies, nil
}

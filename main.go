package main

import "fmt"
import "bufio"
import "os"
import "log"
import "sync"

func data_source(wg *sync.WaitGroup, upstream chan string, downstreams chan string) {
  defer wg.Done()
  wg.Add(1)
  filepath := "datasource/pride_prejudice_urls.txt"
  downstreams <- filepath
  close(downstreams)
}

func load_text(wg *sync.WaitGroup, upstream chan string, downstreams chan string) {
  defer wg.Done()
  wg.Add(1)
  for filepath := range upstream {
    fmt.Printf("reading: %s\n", filepath)

    f, err := os.Open(filepath)
    defer f.Close()
    if err != nil {
      log.Fatal(err)
    }

    scanner := bufio.NewScanner(f)

    for scanner.Scan() {
      url := scanner.Text()
      fmt.Printf("  line: %s\n", url)

      downstreams <- url
    }

    close(downstreams)
  }
}

func fetch_page(wg *sync.WaitGroup, upstream chan string, downstreams chan string) {
  defer wg.Done()
  wg.Add(1)
  for url := range upstream {
    downstreams <- url
  }
  close(downstreams)
}

func soup_parse() {

}

func word_count() {

}

func word_count_merge() {
}

// func connect(wg *sync.WaitGroup, stageFunc func(string) string) {
//   defer wg.Done()
//   wg.Add(1)

//   upstream = make(chan string)

//   for elem := range upstream {

//   }

// }

func main() {
  fmt.Printf("Build pipeline.\n")

  wg := new(sync.WaitGroup)
  ch_1_2 := make(chan string)
  ch_2_3 := make(chan string)
  ch_3_4 := make(chan string)

  // downstreams = []chan int{ch_1_2}
  go data_source(wg, nil, ch_1_2)

  // downstreams = []chan int{ch_2_3}
  go load_text(wg, ch_1_2, ch_2_3)

  // downstreams = []chan int
  go fetch_page(wg, ch_2_3, ch_3_4)

  fmt.Printf("Running pipeline\n")
  for result := range ch_3_4 {
    fmt.Printf("  channel: %s\n", result)
  }

  wg.Wait()
}


package main

import "fmt"
import "bufio"
import "os"
import "log"
import "sync"

func data_source(_ string, downstream chan string) {
  filepath := "datasource/pride_prejudice_urls.txt"
  downstream <- filepath
}

func load_text(filepath string, downstream chan string) {
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

    downstream <- url
  }

}

func fetch_page(url string, downstream chan string) {
  downstream <- url
}

func soup_parse() {

}

func word_count() {

}

func word_count_merge() {
}

func connect(wg *sync.WaitGroup, stageFunc func(string, chan string), upstream chan string) chan string {
  downstream := make(chan string)

  go func() {
    defer wg.Done()
    wg.Add(1)

    if upstream == nil {
      stageFunc("", downstream)
    } else {
      for str_elem := range upstream {
        stageFunc(str_elem, downstream)
      }
    }
    close(downstream)
  }()

  return downstream
}

func main() {
  fmt.Printf("Build pipeline.\n")

  wg := new(sync.WaitGroup)

  ch_1_2 := connect(wg, data_source, nil)
  ch_2_3 := connect(wg, load_text, ch_1_2)
  ch_3_4 := connect(wg, fetch_page, ch_2_3)

  fmt.Printf("Running pipeline\n")

  for result := range ch_3_4 {
    fmt.Printf("  channel: %s\n", result)
  }

  wg.Wait()
}


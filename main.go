package main

import "fmt"
import "bufio"
import "os"
import "log"
import "sync"
import "strings"
import "net/http"
import "encoding/json"
import "io/ioutil"
import "golang.org/x/net/html"
import "golang.org/x/net/html/atom"
import "github.com/yhat/scrape"

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
    fmt.Printf("  url: %s\n", url)

    downstream <- url
  }
}

func fetch_page(url string, downstream chan string) {
  resp, err := http.Get(url)
  defer resp.Body.Close()
  if err != nil {
    log.Fatal(err)
  }

  bytes, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    log.Fatal(err)
  }

  page := string(bytes)

  fmt.Printf("  page: fetched %s\n", url)
  downstream <- page
}

func page_parse(page string, downstream chan string) {
  page_reader := strings.NewReader(page)

  root, err := html.Parse(page_reader)
  if err != nil {
    log.Fatal(err)
  }

  matcher := func(n *html.Node) bool {
    if n.DataAtom == atom.P && n.Parent != nil && n.Parent.Parent != nil {
      return true
    }
    return false
  }

  p_nodes := scrape.FindAll(root, matcher)
  for _, p_node := range p_nodes {
    paragraph := scrape.Text(p_node)
    fmt.Printf("  parse: parsed page\n")
    downstream <- paragraph
  }
}

func word_count(paragraph string, downstream chan string) {
  word_counts := make(map[string]int)

  arr := strings.Fields(paragraph)

  for _, word := range arr {
    downcased := strings.ToLower(word)
    count, ok := word_counts[downcased]
    if ok == true {
      word_counts[downcased] = count + 1
    } else {
      word_counts[downcased] = 1
    }
  }

  jsonString, err := json.Marshal(word_counts)
  if err != nil {
    log.Fatal(err)
  }

  downstream <- string(jsonString)
}

func word_count_merge() {
}

func connect(wg *sync.WaitGroup, stageFunc func(string, chan string), upstream chan string) chan string {
  downstream := make(chan string)

  go func() {
    // defer wg.Done()
    // wg.Add(1)

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
  ch_4_5 := connect(wg, page_parse, ch_3_4)
  ch_5_6 := connect(wg, word_count, ch_4_5)

  fmt.Printf("Running pipeline\n")

  for result := range ch_5_6 {
    fmt.Printf("  result: %s\n", result)
  }

  //wg.Wait()
}


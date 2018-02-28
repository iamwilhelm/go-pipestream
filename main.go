package main

import "fmt"
import "bufio"
import "os"
import "log"

//import "sync"
import "strings"
import "time"
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
  var netClient = &http.Client{
    Timeout: time.Second * 10,
  }

  resp, err := netClient.Get(url)
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

    // TODO find a way to yield results, so we don't have to pass in downstream
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

func word_count_reduce(total_counts string, counts string) string {
  //word_counts := make(map[string]int)

  // unmarshal json

  // merge total with counts

  jsonString := ""
  // jsonString, err := json.Marshal(total_counts)
  // if err != nil {
  //   log.Fatal(err)
  // }

  // it should be the responsibility of reduce to figure out whether or not to 
  // emit the resulting accumulator. We should be able to yield results instead.
  return string(jsonString)
}

func word_count_reduce_trigger(num_elems int) bool {
  return (num_elems == 5)
}

func source(stageFunc func(string, chan string)) chan string {
  downstream := make(chan string)

  go func() {
    stageFunc("", downstream)
    close(downstream)
  }()

  return downstream
}

func flatMap(stageFunc func(string, chan string), upstream chan string) chan string {
	downstream := make(chan string)

	go func() {
		for str_elem := range upstream {
			stageFunc(str_elem, downstream)
		}
		close(downstream)
	}()

	return downstream
}

func reduce(stageFunc func(string, string) string,
  triggerFunc func(int) bool,
	initial string,
	upstream chan string) chan string {

  downstream := make(chan string)

  // iterator
  go func() {
    // initialize for trigger/window
    accumulator := initial
    elem_count := 0

    for str_elem := range upstream {
      // accumulate and reduce 
      accumulator = stageFunc(accumulator, str_elem)
      if triggerFunc(elem_count) {
        // send all results downstream
        downstream <- accumulator
        // reset for trigger/window
        accumulator = initial
        elem_count = 0
      }
    }

    // send the remaining accumulated counts for last window
    downstream <- accumulator
    close(downstream)
  }()

  return downstream
}

func main() {
	fmt.Printf("Build pipeline.\n")

	ch_1_2 := source(data_source)
	ch_2_3 := flatMap(load_text, ch_1_2)
	ch_3_4 := flatMap(fetch_page, ch_2_3)
	ch_4_5 := flatMap(page_parse, ch_3_4)
	ch_5_6 := flatMap(word_count, ch_4_5)
  // ch_6_7 := reduce(word_count_reduce, word_count_reduce_trigger, "{}", ch_5_6)

	fmt.Printf("Running pipeline\n")

	for result := range ch_5_6 {
		fmt.Printf("  result: %s\n", result)
	}
}

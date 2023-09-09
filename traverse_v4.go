package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"pipe_trace_module/github.com/naoina/toml"
)

type tomlConfig struct {
	Title string
	Files struct {
		Network_csv string
	}
}

func main() {
	/*
		the main function reads a csv file containing the graph, traverses all of its edges upstream,
		determines the total upstream length, and builds a list of all upstream edges.

		the graph csv fields are: edge_name, fnode, tnode, and edge_length
	*/
	// read config file
	fmt.Println("Reading input csv and inititializing data structures...")
	f_toml, err := os.Open("traverse_v4.toml")
	if err != nil {
		panic(err)
	}
	defer f_toml.Close()
	var config tomlConfig
	if err := toml.NewDecoder(f_toml).Decode(&config); err != nil {
		panic(err)
	}
	network_csv := config.Files.Network_csv
	// open input csv
	f, _ := os.Open(network_csv)
	defer f.Close()
	// CSVToSingleMap returns a map with a key of edge_name, and a slice containing fnode, tnode, and edge_length
	m_graph := CSVToSingleMap(f)
	n := len(m_graph)
	// outlet_edges := outlets(m_graph)
	// fmt.Println("outlets: ", outlet_edges)
	m_output_length := map[string]string{}
	m_visited := map[string]bool{}
	// m_output_edges and m_usds_edges values are slices of upstream edges from the key edge
	m_output_edges := map[string][]string{}
	m_usds_edges := map[string][]string{}
	// initialize dictionaries
	for k := range m_graph {
		m_output_length[k] = "0"
		m_visited[k] = false
		m_output_edges[k] = make([]string, 0)
		m_usds_edges[k] = make([]string, 0)
	}
	// append 0 to graph slice to hold total us/ds length
	for key := range m_graph {
		m_graph[key] = append(m_graph[key], "0")
	}
	base_edges := make([]string, 1)
	start := time.Now()
	i := 1
	for edge := range m_graph {
		if (i%100 == 0 && n >= 100) || i == 1 || i == n {
			// fmt.Printf("%d of %d (%.2f%%) %s\n", i, n, float32(i)/float32(n)*100, time.Since(start))
			fmt.Printf("%d of %d (%.2f%%) %.2f\n", i, n, float32(i)/float32(n)*100, time.Since(start).Seconds())
		}
		if n < 100 && i > 1 {
			fmt.Printf("%d of %d (%.2f%%) %s\n", i, n, float32(i)/float32(n)*100, time.Since(start))
		}
		// trace upstream if the edge has not already been visited
		if !m_visited[edge] {
			// initially the base_edges slice is just the starting edge
			base_edges[0] = edge
			trace(edge, base_edges, m_graph, m_usds_edges, m_output_length, m_visited, 0)
		}
		m_output_edges[edge] = m_usds_edges[edge]
		i++
	}
	// get the count of upstream edges for each edge
	m_output_edge_count := map[string]string{}
	for edge := range m_output_edges {
		m_output_edge_count[edge] = strconv.Itoa(len(m_output_edges[edge]))
	}
	// write the output to csv files
	WriteToCSV("edges.csv", m_output_edges)
	WriteToCSV2("total_length.csv", m_output_length, "Total Upstream Length")
	WriteToCSV2("upstream_pipe_count.csv", m_output_edge_count, "Count of Upstream Pipes")
}

func trace(starting_edge string, base_edges []string, m map[string][]string, m_edges map[string][]string,
	m_total_length map[string]string, m_vis map[string]bool, depth int) {
	/*
		trace recursively traverses the input graph upstream
		parameters (all maps are passed by reference):
			starting edge: edge from which to start traversal
			base_edges: the starting edge and its upstream edges that have parent edges. used to accumulate lengths
			m: the input map
			m_edges: map of all the edges upstream of the key edge. does not include the key edge itself
			m_total_length: map of total length
			m_vis: map of booleans indicating whether the key edge has been visited
	*/
	//
	// each time the trace function is called, the depth count is incremented
	//   - if called from main (depth initially zero), incremented to 1
	//   - if called recursively, incremented to 2, 3, ...
	//   - if the recursive calls (depth) exceeds 5,000 there is likely a loop in the system and we need to exit the program
	depth++
	if depth > 5000 {
		fmt.Println("It appears the network has a loop. The last edge processed is: ", starting_edge)
		fmt.Println("base edges (portion of network that is looped): ", base_edges)
		os.Exit(99)
	}
	// parent edges are those where their downstream node (tnode) is equal to the starting_edge upstream node (fnode)
	var parents = make([]string, 0)
	for k := range m {
		if m[k][1] == m[starting_edge][0] {
			parents = append(parents, k)
		}
	}
	// if the starting_edge has parents, start traversing
	// the parent edge is added to the "base" edges slice, so as the trace is done on the parents recursively,
	// we are keeping track of related upstream edges and total upstream lengths of the base edges.
	// remember that the base edges include the starting edge.
	if len(parents) > 0 {
		if !contains(base_edges, starting_edge) {
			base_edges = append(base_edges, starting_edge)
		}
		for _, edge := range parents {
			for _, be := range base_edges {
				// add each parents' length to each of the base edges
				var v1, _ = strconv.ParseFloat(m[be][3], 8)
				var v2, _ = strconv.ParseFloat(m[edge][2], 8)
				m[be][3] = fmt.Sprintf("%f", v1+v2)
				m_edges[be] = append(m_edges[be], edge)
				// if parent edge visited, need to add info already known about the parent edge:
				//   - add the lengths of all its upstream edges to the base edge
				//   - add the parent's edges to the base edge's upstream edges
				if m_vis[edge] {
					for _, pe := range m_edges[edge] {
						var v1, _ = strconv.ParseFloat(m[be][3], 8)
						var v2, _ = strconv.ParseFloat(m[pe][2], 8)
						m[be][3] = fmt.Sprintf("%f", v1+v2)
						if !contains(m_edges[be], pe) {
							m_edges[be] = append(m_edges[be], pe)
						}
					}
				}
			}
			// if the parent edge has not been visited, perform a recursive trace
			if !m_vis[edge] {
				trace(edge, base_edges, m, m_edges, m_total_length, m_vis, depth)
			}
		}
		// at this point we're done with this starting_edge
		// pop the last base_edge off the list since we're done with it
		if len(base_edges) > 0 {
			base_edges = base_edges[:len(base_edges)-1]
		}
		// update the total length map and mark the starting_edge as having been visited
		m_total_length[starting_edge] = m[starting_edge][3]
		m_vis[starting_edge] = true
	} else {
		// the starting_edge has no parents, so no upstream edges and a total length of 0
		// nothing to do, so just mark it as having been visited
		m_vis[starting_edge] = true
		return
	}
}

func outlets(m map[string][]string) []string {
	/*
		outlets finds the edges that do not have a downstream edge
		i.e., its tnode is not equal to any other edge's fnode
		returns slice of strings
	*/
	outlet_edges := make([]string, 0)
	for k1, v1 := range m {
		is_outlet := true
		for _, v2 := range m {
			if v1[1] == v2[0] {
				is_outlet = false
			}
		}
		if is_outlet {
			outlet_edges = append(outlet_edges, k1)
		}
	}
	return outlet_edges
}

func contains(s []string, e string) bool {
	/*
		check if string e is in slice s
	*/
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func CSVToSingleMap(reader io.Reader) map[string][]string {
	r := csv.NewReader(reader)
	m := map[string][]string{}
	var header []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if header == nil {
			header = record
		} else {
			vals := make([]string, 0)
			for i := range header {
				if i > 0 {
					vals = append(vals, record[i])
				}
			}
			m[record[0]] = vals
		}
	}
	return m
}

func WriteToCSV(csvfile string, data map[string][]string) {
	/*
		writes map to csv.
		map's value is a slice of strings
	*/
	f, _ := os.Create(csvfile)
	writer := csv.NewWriter(f)

	for k, lst := range data {
		vals := make([]string, 0)
		vals = append(vals, k)
		for _, e := range lst {
			vals = append(vals, e)
		}
		writer.Write(vals)
	}
	writer.Flush()
	f.Close()
}
func WriteToCSV2(csvfile string, data map[string]string, output_field string) {
	/*
		writes map to csv.
		map's value is a single string
	*/
	f, _ := os.Create(csvfile)
	writer := csv.NewWriter(f)
	header := make([]string, 2)
	header[0] = "Pipe ID"
	header[1] = output_field
	writer.Write(header)

	for k, v := range data {
		vals := make([]string, 0)
		vals = append(vals, k)
		vals = append(vals, v)
		writer.Write(vals)
	}
	writer.Flush()
	f.Close()
}

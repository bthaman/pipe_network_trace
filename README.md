# pipe_network_trace
reads a csv file containing a graph of networked pipes (no loops), traverses all of its edges upstream, determines the total upstream length, and builds a list of all upstream edges. 

Test input network_test.csv:
```
edge_name,fnode,tnode,length
A,1,4,50
B,5,2,50
K,2,1,75
C,3,6,50
D,4,5,100
E,6,5,100
F,7,6,100
G,5,9,100
H,8,9,50
I,10,9,50
J,9,11,50
```

### Prerequisites
```
"github.com/naoina/toml"
```
## Built With

* [Go 1.15.5]

## Running the Application
Run from the windows command window:
```
go run traverse_v4.go
```

## Author
* **Bill Thaman** - *Initial work* - [bthaman/pipe_network_trace](https://github.com/bthaman/pipe_network_trace)
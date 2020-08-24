///|
/// Directed Acyclic Graph (DAG) topology builder and validation engine.
///
/// Implements graph node and edge management, DFS cycle detection, and Kahn
/// topological sorting for pipelined streaming execution.

///|
/// Node in the execution graph representing an operator stage.
pub struct DAGNode {
  id : Int
  name : String
  upstream : Array[Int]
  downstream : Array[Int]
}

///|
/// Directed communication edge between upstream and downstream nodes.
pub struct DAGEdge {
  from_node : Int
  to_node : Int
  channel : @buffer.BoundedChannel[@types.StreamRecord]
}

///|
/// Full execution graph containing all nodes, connections, and flow-control channels.
pub struct ExecutionTopology {
  nodes : Array[DAGNode]
  edges : Array[DAGEdge]
}

///|
/// Creates an empty ExecutionTopology.
pub fn ExecutionTopology::new() -> ExecutionTopology {
  { nodes: [], edges: [] }
}

///|
/// Adds a named operator node to the topology. Returns assigned node ID.
pub fn ExecutionTopology::add_node(
  self : ExecutionTopology,
  name : String,
) -> Int {
  let id = self.nodes.length()
  let node : DAGNode = { id, name, upstream: [], downstream: [] }
  self.nodes.push(node)
  id
}

///|
/// Connects from_node to to_node with a bounded channel.
/// Returns true if connection succeeded without creating cycles.
pub fn ExecutionTopology::connect(
  self : ExecutionTopology,
  from_node : Int,
  to_node : Int,
  buffer_capacity : Int,
) -> Bool {
  if from_node < 0 ||
    from_node >= self.nodes.length() ||
    to_node < 0 ||
    to_node >= self.nodes.length() ||
    from_node == to_node {
    return false
  }

  // Check if edge already exists
  for i = 0; i < self.edges.length(); i = i + 1 {
    let e = self.edges[i]
    if e.from_node == from_node && e.to_node == to_node {
      return true
    }
  }

  self.nodes[from_node].downstream.push(to_node)
  self.nodes[to_node].upstream.push(from_node)

  let chan = @buffer.BoundedChannel::new(
    buffer_capacity,
    @buffer.OverflowPolicy::block(),
  )
  self.edges.push({ from_node, to_node, channel: chan })

  // Verify graph remains acyclic
  if self.has_cycle() {
    // Rollback
    let _ = self.nodes[from_node].downstream.pop()
    let _ = self.nodes[to_node].upstream.pop()
    let _ = self.edges.pop()
    false
  } else {
    true
  }
}

///|
/// Checks if the topology contains any directed cycles using DFS coloring.
pub fn ExecutionTopology::has_cycle(self : ExecutionTopology) -> Bool {
  let n = self.nodes.length()
  // 0 = unvisited, 1 = visiting (in stack), 2 = visited
  let state = FixedArray::make(n, 0)

  fn dfs(u : Int) -> Bool {
    state[u] = 1
    let downstream = self.nodes[u].downstream
    for i = 0; i < downstream.length(); i = i + 1 {
      let v = downstream[i]
      if state[v] == 1 {
        return true // Cycle detected!
      }
      if state[v] == 0 && dfs(v) {
        return true
      }
    }
    state[u] = 2
    false
  }

  for i = 0; i < n; i = i + 1 {
    if state[i] == 0 && dfs(i) {
      return true
    }
  }

  false
}

///|
/// Computes topological ordering of nodes using Kahn algorithm.
/// Returns Some(ordered_ids) or None if a cycle exists.
pub fn ExecutionTopology::topological_sort(
  self : ExecutionTopology,
) -> Array[Int]? {
  let n = self.nodes.length()
  let in_degree = FixedArray::make(n, 0)

  for i = 0; i < n; i = i + 1 {
    in_degree[i] = self.nodes[i].upstream.length()
  }

  let queue : Array[Int] = []
  for i = 0; i < n; i = i + 1 {
    if in_degree[i] == 0 {
      queue.push(i)
    }
  }

  let order : Array[Int] = []
  let mut head = 0

  while head < queue.length() {
    let u = queue[head]
    head = head + 1
    order.push(u)

    let downstream = self.nodes[u].downstream
    for i = 0; i < downstream.length(); i = i + 1 {
      let v = downstream[i]
      in_degree[v] = in_degree[v] - 1
      if in_degree[v] == 0 {
        queue.push(v)
      }
    }
  }

  if order.length() == n {
    Some(order)
  } else {
    None
  }
}

///|
/// Returns total nodes in topology.
pub fn ExecutionTopology::node_count(self : ExecutionTopology) -> Int {
  self.nodes.length()
}

///|
/// Returns total edges in topology.
pub fn ExecutionTopology::edge_count(self : ExecutionTopology) -> Int {
  self.edges.length()
}

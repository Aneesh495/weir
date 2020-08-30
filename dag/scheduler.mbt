///|
/// Pipelined topology execution driver and step scheduler.
///
/// Drives event transmission across topological nodes with flow-control channel
/// polling and backpressure coordination.

///|
/// Node execution handler signature taking incoming record and producing outputs.
pub struct NodeHandler {
  handler : (@types.StreamRecord, @operator.Collector) -> Unit
}

///|
/// Creates a NodeHandler from a processing function.
pub fn NodeHandler::new(
  handler : (@types.StreamRecord, @operator.Collector) -> Unit,
) -> NodeHandler {
  { handler, }
}

///|
/// Pipeline execution runtime orchestrating topological stream scheduling.
pub struct PipelineScheduler {
  topology : ExecutionTopology
  handlers : FixedArray[NodeHandler?]
  collectors : FixedArray[@operator.Collector]
}

///|
/// Creates a PipelineScheduler for a validated execution topology.
pub fn PipelineScheduler::new(topology : ExecutionTopology) -> PipelineScheduler {
  let n = topology.node_count()
  let h = FixedArray::make(n, None)
  let c = FixedArray::make(n, @operator.Collector::new())
  for i = 0; i < n; i = i + 1 {
    c[i] = @operator.Collector::new()
  }
  { topology, handlers: h, collectors: c }
}

///|
/// Binds an operator execution handler to a node ID.
pub fn PipelineScheduler::register_handler(
  self : PipelineScheduler,
  node_id : Int,
  handler : NodeHandler,
) -> Unit {
  if node_id >= 0 && node_id < self.handlers.length() {
    self.handlers[node_id] = Some(handler)
  }
}

///|
/// Executes a single pipeline step: processes buffered records through topological order.
/// Returns total records processed in this cycle.
pub fn PipelineScheduler::step(self : PipelineScheduler) -> Int {
  let order_opt = self.topology.topological_sort()
  let order = match order_opt {
    Some(o) => o
    None => return 0
  }

  let mut total_processed = 0

  for idx = 0; idx < order.length(); idx = idx + 1 {
    let node_id = order[idx]
    let h_opt = self.handlers[node_id]

    match h_opt {
      Some(h) => {
        // Collect records from all incoming channels to this node
        let incoming : Array[@types.StreamRecord] = []
        for i = 0; i < self.topology.edges.length(); i = i + 1 {
          let edge = self.topology.edges[i]
          if edge.to_node == node_id {
            let drained = edge.channel.drain()
            for j = 0; j < drained.length(); j = j + 1 {
              incoming.push(drained[j])
            }
          }
        }

        // Process each incoming record
        let col = self.collectors[node_id]
        for i = 0; i < incoming.length(); i = i + 1 {
          (h.handler)(incoming[i], col)
          total_processed = total_processed + 1
        }

        // Forward collected outputs to all downstream outgoing channels
        let outputs = col.drain()
        for i = 0; i < self.topology.edges.length(); i = i + 1 {
          let edge = self.topology.edges[i]
          if edge.from_node == node_id {
            for j = 0; j < outputs.length(); j = j + 1 {
              let _ = edge.channel.push(outputs[j])
            }
          }
        }
      }
      None => ()
    }
  }

  total_processed
}

///|
/// Injects a record directly into an edge targeting a node.
pub fn PipelineScheduler::inject_record(
  self : PipelineScheduler,
  target_node_id : Int,
  record : @types.StreamRecord,
) -> Bool {
  for i = 0; i < self.topology.edges.length(); i = i + 1 {
    let edge = self.topology.edges[i]
    if edge.to_node == target_node_id {
      return edge.channel.push(record)
    }
  }
  false
}

///|
/// Runs the pipeline until all inter-operator channels are drained or max_steps reached.
pub fn PipelineScheduler::run_until_idle(
  self : PipelineScheduler,
  max_steps : Int,
) -> Int {
  let mut steps = 0
  while steps < max_steps {
    let processed = self.step()
    steps = steps + 1
    if processed == 0 {
      break
    }
  }
  steps
}

# Sample project to demonstrate a possible bug in SpringBatch parallel execution
### Steps to reproduce
Run the SpringBatchParallelBug Spring application.
### Expected
The job completes successfully, using two executors for `parallelFlowWorks` and `parallelFlowNoWork`.
### Actual
`parallelFlowWorks` runs just fine, but `parallelFlowNoWork` gets hung after running just two tasks.
### What is different?
`parallelFlowWorks` calls `Flow.split.add(Flow[])` just once with a list of Flows that have been built up.

`parallelFlowNoWork` calls `.add(Flow)` repeatedly to add its list of Flows.

It is unexpected that these would behave differently, but clearly one works and the other does not.

As a side note, it is unfortunate that to enable parallel workers on steps, that each Step must be
wrapped in a Flow.  Why does this requirement exist?

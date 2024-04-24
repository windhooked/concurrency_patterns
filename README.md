# Concurrency patterns

This is my playground to compare and contrast some concurrency patterns I commonly use 

A typical recuring processing pattern is batching up events before fan-out to workers and then collecting results.

The folders: 
batch contains an implementation based on [reactiveX](http://github.com/reactivex/rxgo/v2), worker contains a pure implementation based on go functions and channels

https://go.dev/blog/pipelines


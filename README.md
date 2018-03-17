# What is this?

This was just a test of how a streaming pipeline would work and look like.

Just builds a linear pipeline downloading parts of Shelley's Frankenstein off
of the web and counting the words.

# What was done?

To simulate streaming, I just used go channels. While I was able to see 
that it would work, channels are only for a single machine with multiple
cores. It wouldn't work so well in a distributed situation.

Also, before this happens, picking a unified data format would make life easier,
as Go is typed. So for this test, I just used JSON to serialize and unserialize.

In practice, this would wreck havoc on throughput.

# Conclusion

Deciding on a message format will help a lot with the implementation. Also, it
would make more sense to have an in-memory key value / streaming database that
we can write messages to. That way, if we want, we can have the option to 
sync with other nodes, using Bloom latticies.

# Running it

To run, just enter the following at the command line:

```
go run main.go
```


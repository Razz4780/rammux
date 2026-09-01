This crate is for benchmarking rammux against other multiplexing protocols.
The end goal is to have a multi-purpose CLI tool that can be used to:
1. Run any side of a multiplexed network connection (supporting various protocols - rammux, yamux, smux, h2)
2. Generate a TLS bundle to be used for encrypting the traffic between the server and the client
3. Run experiments in a Kubernetes cluster, with Chaos Mesh.

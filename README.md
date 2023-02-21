# ChainReplication
A Distributed System Chain Replication Program

This project is a Chain Replication program. It runs on the command line, and each server running this program serves as a "node" within a "chain".
The program connects with a ZooKeeper server to create a sequential node, and looks at the ZooKeeper server to see where its position in the chain is.
If it is the head of the chain, it takes writes from clients, and passes writes down the chain to be replicated.
If it is the tail of the chain, it allows reads from cliensts, and performs state transfers to new nodes joining the end of the chain.
If it is neither the head or the tail, it takes writes coming down the chain, replicates the write, and passes the write on to the next node.
If a node crashes, and it loses its connection with the ZooKeeper server, or any new node joins the chain, all nodes will detect a change and recalculate
thier position within the chain to determine their role and responsibilities.
This program was written using Java, gRPC, and Apache ZooKeeper. It uses leadered replication to create a system of servers that is capable of handling crashes.

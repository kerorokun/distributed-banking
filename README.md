# Distributed Banking Application
This is a small ACID banking application program designed as a learning project. This banking application uses the RAFT consensus algorithm, a timestamped based concurrency protocol, and the two-phase commit protocol. 

## Application Diagram
For those interested, the way the application is organized in the following:
<img src="https://github.com/kerorokun/distributed-banking/blob/master/images/overall_organization.png" width="700" />

where each cluster is composed of a variable amount of nodes and powered by the RAFT consensus algorithm. As such, each cluster will have a leader that may shift around in the group according to RAFT.

## Running the Program
To run the program:
1. Launch a branch cluster (Repeat for each branch you want to have): 
    * For each node in the cluster, from the base directory run:
      ```
      >> python -m branch_main --ip <ip of the node> --port <port of the node> --id <id of the node> --branch <name of the branch> --branches <ip of other nodes> <port of other nodes> ...
      ```
    
    * Example: Branch cluster with 3 branch nodes at `localhost:8000`, `localhost:8001`, and `localhost:8002`
      ```
      >> python -m branch_main --ip localhost --port 8000 --id branch_A_1 --branch branch_A --branches localhost 8001 localhost 8002
      >> python -m branch_main --ip localhost --port 8001 --id branch_A_2 --branch branch_A --branches localhost 8000 localhost 8002
      >> python -m branch_main --ip localhost --port 8002 --id branch_A_3 --branch branch_A --branches localhost 8000 localhost 8001
      ```
2. Launch a coordinator cluster: 
    * For each node in the cluster, from the base directory run: 
      ```
      >> python -m coordinator_main --ip <ip of the node> --port <port of the node> --id <id of the node> --coordinators <ip of other nodes> <port of other nodes> ... --branches <ip of a node in a branch> <port of a node in a branch> ...
      ```
    * Example: Coordinator cluster with 3 coordinator nodes at `localhost:7000`, `localhost:7001`, and `localhost:7002` in a scenario with 2 bank branch clusters A and B at addresses `localhost:8000 -> localhost:8003` and `localhost:9000 -> localhost:9003`:
    
      ```
      >> python -m coordinator_main --ip localhost --port 7000 --id coord_1 --coordinators localhost 7001 localhost 7002 --branches localhost 8000 localhost 9000
      >> python -m coordinator_main --ip localhost --port 7001 --id coord_2 --coordinators localhost 7000 localhost 7002 --branches localhost 8000 localhost 9000
      >> python -m coordinator_main --ip localhost --port 7002 --id coord_3 --coordinators localhost 7000 localhost 7001 --branches localhost 8000 localhost 9000
      ```
3. Launch a client (Repeat for each client you want to have): 
    * For each client, from the base directory run: 
      ```
      >> python -m client_main --ip <ip of the client> --port <port of the client> --coordinator <ip of a coordinator node> <port of a coordinator node>
      ```
    * Example: To run a client at `localhost:8080` with a Coordinator cluster in the addresses range of `localhost:7000 -> localhost:7003`:
      ```
      >> python -m client_main --ip localhost --port 8080 --coordinator localhost 7000
      ```

The following commands are available for a client:
* `BEGIN`: Start a new transaction.
* `DEPOSIT <branch name> <account name> <amount>`: Deposit an amount of money into an account at a branch. A transaction must be started.
* `WITHDRAW <branch name> <account name> <amount>`: Withdraw an amount of money into an account at a branch. A transaction must be started.
* `BALANCE <branch name> <account name>`: Get the balance of an account at a branch. A transaction must be started.
* `COMMIT`: Commit the current transaction. A transaction must be started.
* `ABORT`: Abort the current transaction. A transaction must be started.

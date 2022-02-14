# OneDataShare Transfer Service
This tool is the OneDataShare Transfer Service that is responsible for the physical sending of files.

## CSE-603 PDP Project 

#####Contributors

####Links to Relevant Papers

####The Problem
Currently, the OneDataShare Transfer-Service do not collect/report(to AWS deployment) the network state they experience. Tools such as "sar" and "ethtool" report metrics like: ping, bandwidth, latency, link capacity, RTT,, etc to the user that allow them to understand bottlenecks in their network.

####Metrics we collect
1. Kernel Level: 
   * active cores
   * cpu frequency
   * energy consumption
   * cpu Architecture 
2. Application Level: 
   * pipelining
   * concurrency
   * parallelism
   * chunk size
3. Network Level:
   * RTT
   * Bandwidth 
   * BDP(Link capacity * RTT)
   * packet loss rate
   * link capacity
4. Data Characteristics: 
   * Number of files
   * Total Size of transfer
   * Average file size
   * std deviation of file sizes
   * file types in the transfer
    
End System Resource Usage: 
   * % of CPU’s used 
   * % of NIC used.

####Potential Solutions
We have come up with 3 potential solutions to aggregating these metrics per Transfer Service
1. Writing a python script which 
####Challenges per Solution


####Libraries to be used per solution

#####List of Technologies

####What we will Achomplish



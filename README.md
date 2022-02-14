# OneDataShare Transfer Service
This tool is the OneDataShare Transfer Service that is responsible for the physical sending of files.

## CSE-603 PDP Project
#####Contributors
Deepika Ghodki
Aman Harsh
Neha Mishra
Jacob Goldverg

####Links to Relevant Papers
1. [Historical Analysis and Real-Time Tuning](https://cse.buffalo.edu/faculty/tkosar/papers/jrnl_tpds_2018.pdf)
2. Cheng, Liang and Marsic, Ivan. ‘Java-based Tools for Accurate Bandwidth Measurement of Digital Subscriber Line Networks’. 1 Jan. 2002 : 333 – 344.
3. [Java-based tools for accurate bandwidth measurement of Digital Subscriber Line](https://www.researchgate.net/publication/237325992_Java-based_tools_for_accurate_bandwidth_measurement_of_Digital_Subscriber_Line)
4. [Energy-saving Cross-layer Optimization of Big Data Transfer Based on Historical Log Analysis](https://arxiv.org/pdf/2104.01192.pdf)
5. [Cross-layer Optimization of Big Data Transfer Throughput and Energy Consumption](https://par.nsf.gov/servlets/purl/10113313)
6. [HARP: Predictive Transfer Optimization Based on Historical Analysis and Real-time Probing](https://cse.buffalo.edu/faculty/tkosar/papers/sc_2016.pdf)

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

#### Potential Solutions
We have come up with 3 potential solutions to aggregating these metrics per Transfer Service.
Solution 1. Writing a Python script which the Transfer-Service will run as a CRON job to collect the network conditions periodically. The script will create a file that will be formatted metric report, and the Transfer-Service will then read/parse that file and send it to CockroachDB/Prometheous that will be run on the AWS backend
Solution 2. We detect the Operating System and use the standard networking tools available to us and do string parsing to then construct a metrics report and send to CockroachDB/Prometheous
Solution 3. Implement the collection of these metrics on the connections/streams that the Transfer Service creates thus a totally manual implementation. As the metrics are collected we will report them back to AWS as a report.

#### Challenges per Solution

Solution 1: We currently expect the actual metrics to not be as accurate as the manual implementation on the Java application. As UDP/TCP are dynamic we know that having separate connections(python sockets vs java sockets) will create variability in the measurements. Another source of variability is using another programming language will only provide an estimation of what the Transfer-Service is experiencing in performance as the Java is completely virtualized. 
The benefit of this approach is that Python has many libraries more network measuring libraries.  

Solution 2:
We currently believe that this approach would result in a large amount of manual string parsing which is ok but will be challenging intially.

Solution 3:
This solution will be the most complex programmatically as we would need to introduce new classes that would allow the wrapping of streams.

#### Libraries to be used per solution
ping: will allow measurement of packet loss and latency
statsd: A library that allows us to construct concise reports for sending to AWS.

Solution 1. tcp-latency, udp-latency, ping, psutil(Exposes: CPU, NIC metrics) allows us to compute RTT, Bandwidth, estimated link capacity.

Solution 2: 
1. Windows(ping) 
2. Unix(ping, iftop)
3. Mac(ping, iftop)

Solution 3:
A manual implementation will require us wrapping the InputStream, OutputStream, and respective Channels such that we are able to keep track of the data sent/rcv vs how much we expected to send/rcv.

##### List of Technologies
Tools: ping, iftop
Technologies: Java, Python, CockroachDB, Prometheus, Grafana

#### What we will Accomplish
By the end of the semester we would like to have the transfer service to be fully monitoring its network conditions and reporting it periodically back to the ODS backend.
We will be using either CockroachDB or Prometheus to be storing the time-series data thus allowing the ODS deployment to optimize the transfer based on the papers above.
For extra browny points we would like to implement a Grafana dashboard so every user can be aware of the network conditions around their transfer.
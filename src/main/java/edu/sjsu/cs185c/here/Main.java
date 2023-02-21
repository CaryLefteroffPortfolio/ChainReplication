package edu.sjsu.cs185c.here;

import edu.sjsu.cs249.chain.*;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class Main {

    enum Role {
        HEAD,
        REPLICA,
        TAIL,
        HEADandTAIL
    }

    //CONSTANT RESPONSES
    public static AckResponse ackResponse = AckResponse.newBuilder().build();
    public static StateTransferResponse stateTransferResponseSuccess = StateTransferResponse.newBuilder().setRc(0).build();
    public static StateTransferResponse stateTransferResponseFail = StateTransferResponse.newBuilder().setRc(1).build();
    public static UpdateResponse updateResponseSuccess = UpdateResponse.newBuilder().setRc(0).build();
    public static UpdateResponse updateResponseFail = UpdateResponse.newBuilder().setRc(1).build();

    //SHOULD NEVER CHANGE AFTER BEING INITIALIZED
    public static int port;
    public static String hostPort;
    public static final String name = "Cary";
    public static ZooKeeper zk;
    public static String basePath;

    //SHOULD RARELY CHANGE
    public static Role state = null;
    public static String hostPortNext = "";
    public static String hostPortPrev = "";

    //SHOULD FREQUENTLY CHANGE
    public static int xid = 0;
    public static int lastAckedXid = 0;
    public static Map<String, Integer> map = new HashMap<>();
    public static Map<Integer, Semaphore> xidSems = new HashMap<>();
    public static Map<Integer, UpdateRequest> sentList = new HashMap<>();

    //STUBS
    public static Map<String, ReplicaGrpc.ReplicaBlockingStub> repstubs = new HashMap<>();

    public static void searchState() {
        boolean success = true;
        try {
            List<String> nodes = zk.getChildren(basePath, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    determineState();
                }
            });
            Collections.sort(nodes);
            for(String s: nodes) { System.out.println(s);}
            Stat stat = new Stat();
            for(int i = 0; i < nodes.size(); i++) {
                byte[] res = zk.getData(basePath + "/" + nodes.get(i), false, stat);
                String data = new String(res);
                String[] strArr = data.split("\n");
                System.out.println(i + " --- " + strArr[0] + " --- " + strArr[1]);
                if(!(strArr[0].equals(hostPort) && strArr[1].equals(name))) {
                    continue;
                }
                System.out.println("Found self in search state at index " + i);
                if(i > 0) {
                    byte[] prevRes = zk.getData(basePath + "/" + nodes.get(i - 1), false, stat);
                    String prevData = new String(prevRes);
                    hostPortPrev = prevData.split("\n")[0];
                    System.out.println("Set predecessor: " + hostPortPrev);
                } else {
                    System.out.println("No predecessor detected.");
                }
                if(i < nodes.size() - 1) {
                    byte[] nextRes = zk.getData(basePath + "/" + nodes.get(i + 1), false, stat);
                    String nextData = new String(nextRes);
                    hostPortNext = nextData.split("\n")[0];
                    System.out.println("Set successor: " + hostPortNext);
                } else {
                    System.out.println("No successor detected.");
                }
                break;
            }
            if(hostPortNext.equals("DNE") || hostPortPrev.equals("DNE")) {
                if(hostPortNext.equals("DNE") && hostPortPrev.equals("DNE")) {
                    state = Role.HEADandTAIL;
                    System.out.println("CURRENT ROLE: HEAD AND TAIL");
                } else if(hostPortNext.equals("DNE")) {
                    state = Role.TAIL;
                    System.out.println("CURRENT ROLE: TAIL");
                } else {
                    state = Role.HEAD;
                    System.out.println("CURRENT ROLE: HEAD");
                }
            } else {
                state = Role.REPLICA;
                System.out.println("CURRENT ROLE: REPLICA");
            }
        } catch (InterruptedException e){
            System.out.println("Interrupted Exception on zk.getChildren() in determineState.");
            success = false;
        } catch (KeeperException e) {
            System.out.println("Keeper Exception on zk.getChildren() in determineState.");
            e.printStackTrace();
            success = false;
        }
        if(!success) {
            try {
                System.out.println("Error enountered in determining state. Retrying...");
                Thread.sleep(2000);
            } catch (InterruptedException e) { System.out.println("Interrupted Exception in 2 second sleep in Search State");}
            searchState();
        }
    }

    public static void determineState() {
        System.out.println("Change detected. Redetermining state.");
        hostPortNext = "DNE";
        hostPortPrev = "DNE";
        searchState();
        if(state != Role.HEADandTAIL && state != Role.TAIL) { //If we are not the tail, send a state transfer
            var channel = ManagedChannelBuilder.forTarget(hostPortNext).usePlaintext().build();
            //var stub = ReplicaGrpc.newBlockingStub(channel);
            ReplicaGrpc.ReplicaBlockingStub stub = null;
            if(repstubs.containsKey(hostPortNext)) {
                stub = repstubs.get(hostPortNext);
            } else {
                stub = ReplicaGrpc.newBlockingStub(channel);
                repstubs.put(hostPortNext, stub);
            }
            var sentJournal = new LinkedList<UpdateRequest>(sentList.values());
            var transferRequest = StateTransferRequest.newBuilder()
                    .setXid(lastAckedXid)
                    .addAllSent(sentJournal)
                    .putAllState(map)
                    .build();
            boolean loop = true;
            while(loop) {
                loop = false;
                try {
                    var tranferResponse = stub.stateTransfer(transferRequest);
                    System.out.println("Sent transfer request. Contents of sent list:");
                    for(UpdateRequest u : sentJournal) {
                        System.out.println("KEY " + u.getKey() + ", VAL " + u.getNewValue());
                    }
                } catch (StatusRuntimeException e) {
                    loop = true;
                }
            }
        }
    }

    public void updateMap(String key, int val) {
        if(map.containsKey(key)) {
            synchronized(this) {
                int currVal = map.get(key);
                map.put(key, currVal + val);
            }
        } else {
            synchronized (this) {
                map.put(key, val);
            }
        }
    }

    @Command(name = "Chain Replication", subcommands = {CliClient.class, CliServer.class})
    static class TopCommand { }
    @Command(name = "ChainClient", mixinStandardHelpOptions = true, description = "Chain Replication")
    static class CliClient implements Callable<Integer> {
        @Parameters(index = "0", description = "server to connect to.")
        String serverPort;

        @Override
        public Integer call() throws Exception {
            return 0;
        }
    }

    static class HeadChainReplicaImpl extends HeadChainReplicaGrpc.HeadChainReplicaImplBase {

        @Override
        public void increment(IncRequest request, StreamObserver<HeadResponse> responseObserver) {
            System.out.println("Received Inc Request");
            HeadResponse response = null;
            if(state != Role.HEAD && state != Role.HEADandTAIL) {
                response = HeadResponse.newBuilder().setRc(1).build(); //I'm not the head
            } else {
                int currXID = 0;
                synchronized (this) {
                    xid++;
                    currXID = xid;
                }
                String key = request.getKey();
                Main temp = new Main();
                temp.updateMap(key, request.getIncValue());
                System.out.println("As Head: Incremented Key " + key + " by " + request.getIncValue() + " in XID " + xid);
                if(state == Role.HEAD) {
                    try {
                        var channel = ManagedChannelBuilder.forTarget(hostPortNext).usePlaintext().build();
                        ReplicaGrpc.ReplicaBlockingStub stub = null;
                        if(repstubs.containsKey(hostPortNext)) {
                            stub = repstubs.get(hostPortNext);
                        } else {
                            stub = ReplicaGrpc.newBlockingStub(channel);
                            repstubs.put(hostPortNext, stub);
                        }
                        var updateRequest = UpdateRequest.newBuilder()
                                .setKey(key)
                                .setNewValue(map.get(key))
                                .setXid(xid).build();
                        synchronized (this) {
                            sentList.put(currXID, updateRequest);
                        }
                        boolean loop = true;
                        while (loop) {
                            loop = false;
                            try {
                                var updateResponse = stub.update(updateRequest);
                            } catch (StatusRuntimeException e) {
                                try {
                                    Thread.sleep(2000);
                                } catch (InterruptedException err) {
                                }
                                loop = true;
                            }
                        }
                        xidSems.put(currXID, new Semaphore(1));
                        System.out.println("As Head: XID " + xid + " waiting for an Ack.");
                        xidSems.get(currXID).wait();
                    } catch (InterruptedException e) {
                        System.out.println("Interrupted Exception in Incremenet.");
                    }
                }
                lastAckedXid = Math.max(lastAckedXid, currXID);
                response = HeadResponse.newBuilder().setRc(0).build(); //Success
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    static class ReplicaImpl extends ReplicaGrpc.ReplicaImplBase {

        public Map<String, ReplicaGrpc.ReplicaBlockingStub> stubs;
        @Override
        public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
            System.out.println("Received Update Request");
            synchronized(this) {
                map.put(request.getKey(), request.getNewValue());
            }
            int currXID = 0;
            synchronized (this) {
                xid = request.getXid();
                currXID = xid;
            }
            System.out.println("Updated Key " + request.getKey() + " to Value " + request.getNewValue() + " as XID " + xid);
            responseObserver.onNext(updateResponseSuccess);
            responseObserver.onCompleted();

            if(state != Role.TAIL) { //If we are not the tail, send the update down the chain
                synchronized (this) {
                    sentList.put(currXID, request);
                }
                var channel = ManagedChannelBuilder.forTarget(hostPortNext).usePlaintext().build();
                ReplicaGrpc.ReplicaBlockingStub stub = null;
                if(repstubs.containsKey(hostPortNext)) {
                    stub = repstubs.get(hostPortNext);
                } else {
                    stub = ReplicaGrpc.newBlockingStub(channel);
                    repstubs.put(hostPortNext, stub);
                }
                boolean loop = true;
                System.out.println("Sending update request down the chain for XID " + currXID);
                while (loop) {
                    loop = false;
                    try {
                        var updateResponse = stub.update(request);
                    } catch (StatusRuntimeException e) {
                        try {
                            Thread.sleep(2000);
                        } catch(InterruptedException err) {
                        }
                        loop = true;
                    }
                }
                try {
                    synchronized (this) {
                        xidSems.put(currXID, new Semaphore(1));
                    }
                    System.out.println("As Head: XID " + xid + " waiting for an Ack.");
                    synchronized (this) {
                        xidSems.get(currXID).wait();
                    }
                } catch (InterruptedException e) {}
            } else { //If we are the tail, so send an Ack back
                System.out.println("As Tail: Sending Ack back up the chain for XID " + currXID);
                lastAckedXid = currXID;
                var channel = ManagedChannelBuilder.forTarget(hostPortPrev).usePlaintext().build();
                ReplicaGrpc.ReplicaBlockingStub stub = null;
                if(repstubs.containsKey(hostPortNext)) {
                    stub = repstubs.get(hostPortNext);
                } else {
                    stub = ReplicaGrpc.newBlockingStub(channel);
                    repstubs.put(hostPortNext, stub);
                }
                var ackRequest = AckRequest.newBuilder().setXid(currXID).build();
                boolean loop = true;
                while(loop) {
                    loop = false;
                    try {
                        var ackResponse = stub.ack(ackRequest);
                    } catch (StatusRuntimeException e) {
                        try {
                            Thread.sleep(2000);
                        } catch(InterruptedException err) {
                        }
                        loop = true;
                    }
                }
            }
        }

        @Override
        public void stateTransfer(StateTransferRequest request, StreamObserver<StateTransferResponse> responseObserver) {
            System.out.println("Received State Transfer Request");
            StateTransferResponse response = null;
            if(state != Role.TAIL && state != Role.HEADandTAIL) {
                System.out.println("Rejected state transfer request because state is not Tail.");
                response = stateTransferResponseFail;
            } else {
                System.out.println("Approved state transfer request. Updating data...");
                Map<String, Integer> newMap = request.getStateMap();
                List<String> keys = new LinkedList<>(newMap.keySet());
                for(String k : keys) {
                    synchronized (this) {
                        System.out.println("Adding KEY " + k + " VAL " + newMap.get(k));
                        map.put(k, newMap.get(k));
                    }
                }
                synchronized (this) {
                    xid = request.getXid();
                }
                var list = request.getSentList();
                for (var item : list) {
                    synchronized (this) {
                        System.out.println("KEY = " + item.getKey());
                        System.out.println("VAL = " + item.getNewValue());
                        map.put(item.getKey(), item.getNewValue()); //ACK BACK
                        var channel = ManagedChannelBuilder.forTarget(hostPortPrev).usePlaintext().build();
                        ReplicaGrpc.ReplicaBlockingStub stub = null;
                        if(repstubs.containsKey(hostPortNext)) {
                            stub = repstubs.get(hostPortNext);
                        } else {
                            stub = ReplicaGrpc.newBlockingStub(channel);
                            repstubs.put(hostPortNext, stub);
                        }
                        var ackRequest = AckRequest.newBuilder().setXid(xid).build();
                        boolean loop = true;
                        while(loop) {
                            loop = false;
                            try {
                                var ackResponse = stub.ack(ackRequest);
                            } catch (StatusRuntimeException e) {
                                try {
                                    Thread.sleep(2000);
                                } catch(InterruptedException err) {
                                }
                                loop = true;
                            }
                        }
                    }
                }
                response = stateTransferResponseSuccess;
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void ack(AckRequest request, StreamObserver<AckResponse> responseObserver) {
            System.out.println("Received Ack Request");
            int ackXID = request.getXid();
            responseObserver.onNext(ackResponse);
            responseObserver.onCompleted();
            lastAckedXid = Math.max(lastAckedXid, ackXID);
            if(state == Role.HEAD) {
                System.out.println("As Head: Received ACK for XID " + ackXID);
                if (xidSems.containsKey(ackXID)) {
                    synchronized (this) {
                        sentList.remove(ackXID);
                        xidSems.get(ackXID).notify();
                        xidSems.remove(ackXID);
                    }
                }
            } else {
                System.out.println("Sending ACK up the chain for XID " + ackXID);
                //If we are not the head, send the Ack back up the chain
                var channel = ManagedChannelBuilder.forTarget(hostPortPrev).usePlaintext().build();
                ReplicaGrpc.ReplicaBlockingStub stub = null;
                if(repstubs.containsKey(hostPortNext)) {
                    stub = repstubs.get(hostPortNext);
                } else {
                    stub = ReplicaGrpc.newBlockingStub(channel);
                    repstubs.put(hostPortNext, stub);
                }
                var ackRequest = AckRequest.newBuilder().setXid(ackXID).build();
                boolean loop = true;
                while(loop) {
                    loop = false;
                    try {
                        var ackResponse = stub.ack(ackRequest);
                    } catch (StatusRuntimeException e) {
                        loop = true;
                    }
                }
            }
        }
    }

    static class TailChainReplicaImpl extends TailChainReplicaGrpc.TailChainReplicaImplBase {
        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            System.out.println("Received Get Request for key " + request.getKey());
            GetResponse response = null;
            if(!map.containsKey(request.getKey())) {
                response = GetResponse.newBuilder().setRc(0).setValue(0).build();
            } else if(state != Role.HEADandTAIL && state != Role.TAIL) {
                response = GetResponse.newBuilder().setRc(1).build();
            } else {
                response = GetResponse.newBuilder().setRc(0).setValue(map.get(request.getKey())).build();
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    static class ChainDebug extends ChainDebugGrpc.ChainDebugImplBase {
        @Override
        public void debug(ChainDebugRequest request, StreamObserver<ChainDebugResponse> responseObserver) {
            System.out.println("Debug request received");
            ChainDebugResponse response = ChainDebugResponse.newBuilder()
                    .setXid(xid)
                    .putAllState(map)
                    .addAllLogs(new LinkedList<>())
                    .addAllSent(new LinkedList<>())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {
            System.out.println("Exit request received");
            System.exit(0);
        }
    }

    @Command(name = "ChainServer", mixinStandardHelpOptions = true, description = "Chain Replication")
    static class CliServer implements Callable<Integer> {
        @Parameters(index = "0", description = "ZK Host:Port")
        String zkServer;

        @Parameters(index = "1", description = "ZK Node Path")
        String path;

        @Parameters(index = "2", description = "My Host:Port")
        String myHostPort;

        @Override
        public Integer call() throws Exception {
            basePath = path;
            hostPort = myHostPort;
            String[] temp = myHostPort.split(":");
            port = Integer.parseInt(temp[1]);
            var server = ServerBuilder.forPort(port)
                    .addService(new HeadChainReplicaImpl())
                    .addService(new ReplicaImpl())
                    .addService(new TailChainReplicaImpl())
                    .addService(new ChainDebug())
                    .build();
            server.start();
            zk = new ZooKeeper(zkServer, 10000, (e) -> {System.out.println(e);});
            String data = myHostPort + "\n" + name;
            boolean connected = false;
            while(!connected) {
                try {
                    zk.create(path + "/replica-", data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                    connected = true;
                } catch(KeeperException e) {
                    connected = false;
                    System.out.println("Creating failed. Retrying...");
                    Thread.sleep(2000);
                }
            }
            System.out.println("ZNode Created.");
            determineState();
            server.awaitTermination();
            return 0;
        }
    }
    public static void main(String[] args) {
        System.exit(new CommandLine(new TopCommand()).execute(args));
    }
}

using Unity.Jobs;
using Unity.Burst;
using Unity.Entities;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Mathematics;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;

namespace ZG
{
    public struct NetworkChatServer
    {
        private struct Identity
        {
            public uint id;
            public ulong channel;
            public FixedString32Bytes name;
        }

        private struct Result
        {
            public NetworkConnection connection;
            public ulong channel;
            public uint id;
            public UnsafeBlock block;

            public DataStreamReader stream => new DataStreamReader(block.AsArray<byte>());
        }

        [BurstCompile]
        private struct AcceptConnections : IJob
        {
            public NetworkDriver driver;

            public NativeBuffer buffer;

            public NativeList<NetworkConnection> connections;

            public NativeParallelMultiHashMap<NetworkConnection, Result> talkingResults;

            public void Execute()
            {
                NetworkConnection connection;
                while ((connection = driver.Accept()) != default(NetworkConnection))
                    connections.Add(connection);

                int numEvents = 0, numConnections = connections.Length;
                for (int i = 0; i < numConnections; i++)
                    numEvents += math.max(1, driver.GetEventQueueSizeForConnection(connections[i]));

                talkingResults.Clear();
                talkingResults.Capacity = math.max(talkingResults.Capacity, numEvents);

                buffer.Reset();
                buffer.capacity = math.max(buffer.capacity, numEvents * driver.MaxPayloadCapacity());
            }
        }

        [BurstCompile]
        private struct PopEvents : IJobParallelForDefer
        {
            public StreamCompressionModel model;

            public NetworkDriver.Concurrent driver;

            public NativeBuffer.ParallelWriter buffer;

            [ReadOnly]
            public NativeArray<NetworkConnection> connections;

            [ReadOnly]
            public NativeParallelMultiHashMap<NetworkConnection, Identity> connectionIdentitis;

            [ReadOnly]
            public NativeParallelMultiHashMap<ulong, NetworkConnection> channelConnections;

            public NativeParallelMultiHashMap<NetworkConnection, Result>.ParallelWriter talkingResults;

            public NativeQueue<Result>.ParallelWriter leaveOrJoinResults;

            public unsafe void Execute(int index)
            {
                Result result;
                result.connection = connections[index];
                if (NetworkConnection.State.Disconnected == driver.GetConnectionState(result.connection))
                {
                    __Disconnect(result.connection, default);

                    return;
                }

                NetworkChatMessageType messageType;
                NetworkEvent.Type cmd;
                DataStreamReader stream;
                while (true)
                {
                    cmd = driver.PopEventForConnection(result.connection, out stream);
                    switch (cmd)
                    {
                        case NetworkEvent.Type.Empty:
                            return;
                        case NetworkEvent.Type.Data:
                            do
                            {
                                messageType = (NetworkChatMessageType)stream.ReadPackedUInt(model);
                                switch (messageType)
                                {
                                    case NetworkChatMessageType.Leave:
                                        result.channel = stream.ReadPackedULong(model);
                                        result.id = 0;
                                        result.block = default;
                                        leaveOrJoinResults.Enqueue(result);
                                        break;
                                    case NetworkChatMessageType.Join:
                                        result.id = stream.ReadPackedUInt(model);
                                        result.channel = stream.ReadPackedULong(model);

                                        var name = stream.ReadFixedString32();

                                        if (result.id == 0)
                                            UnityEngine.Debug.LogError($"Join: the id of channel {result.channel} is 0!");
                                        else
                                        {
                                            result.block = buffer.WriteBlock(UnsafeUtility.SizeOf<ushort>() + name.Length, false);

                                            var writer = new DataStreamWriter(result.block.AsArray<byte>());
                                            writer.WriteFixedString32(name);

                                            leaveOrJoinResults.Enqueue(result);
                                        }
                                        break;
                                    case NetworkChatMessageType.Talk:
                                    case NetworkChatMessageType.Whisper:
                                        uint id = 0;
                                        if (messageType == NetworkChatMessageType.Whisper)
                                            id = stream.ReadPackedUInt(model);

                                        result.channel = stream.ReadPackedULong(model);

                                        result.block = buffer.WriteBlock(stream.Length - stream.GetBytesRead(), false);
                                        stream.ReadBytes(result.block.AsArray<byte>());

                                        result.id = 0;
                                        foreach (var identity in connectionIdentitis.GetValuesForKey(result.connection))
                                        {
                                            if (identity.channel == result.channel)
                                            {
                                                result.id = identity.id;

                                                break;
                                            }
                                        }

                                        if (result.id == 0)
                                            UnityEngine.Debug.LogError($"Talk: the channel {result.channel} has not been joined!");
                                        else if(messageType == NetworkChatMessageType.Talk)
                                        {
                                            foreach (var connection in channelConnections.GetValuesForKey(result.channel))
                                                talkingResults.Add(connection, result);
                                        }
                                        else
                                        {
                                            bool isContains = false;
                                            foreach (var connection in channelConnections.GetValuesForKey(result.channel))
                                            {
                                                foreach (var identity in connectionIdentitis.GetValuesForKey(connection))
                                                {
                                                    if (identity.channel == result.channel && identity.id == id)
                                                    {
                                                        talkingResults.Add(connection, result);

                                                        isContains = true;

                                                        break;
                                                    }

                                                    if (isContains)
                                                        break;
                                                }
                                            }
                                        }

                                        break;
                                }
                            } while (stream.GetBytesRead() < stream.Length);
                            break;
                        case NetworkEvent.Type.Connect:
                            break;
                        case NetworkEvent.Type.Disconnect:
                            __Disconnect(result.connection, stream);
                            break;
                    }
                }
            }

            private void __Disconnect(in NetworkConnection connection, DataStreamReader stream)
            {
                Result result;
                result.block = default;
                result.id = 0;
                result.connection = connection;
                foreach(var identity in connectionIdentitis.GetValuesForKey(connection))
                {
                    result.channel = identity.channel;

                    leaveOrJoinResults.Enqueue(result);
                }
            }
        }

        [BurstCompile]
        private struct ApplyLeaveOrJoinResults : IJob
        {
            public NetworkDriver driver;

            public NativeList<NetworkConnection> connections;

            public NativeQueue<Result> leaveOrJoinResults;

            public NativeParallelMultiHashMap<NetworkConnection, Identity> connectionIdentitis;

            public NativeParallelMultiHashMap<ulong, NetworkConnection> channelConnections;

            public void Execute()
            {
                int numConnections = connections.Length;
                for (int i = 0; i < numConnections; ++i)
                {
                    if (NetworkConnection.State.Disconnected == driver.GetConnectionState(connections[i]))
                    {
                        connections.RemoveAtSwapBack(i--);

                        --numConnections;
                    }
                }

                bool isContains;
                NetworkConnection targetConnection;
                Identity identity;
                NativeParallelMultiHashMapIterator<NetworkConnection> connectionIterator;
                NativeParallelMultiHashMapIterator<ulong> channelIterator;
                while (leaveOrJoinResults.TryDequeue(out var leaveOrJoinResult))
                {
                    if(leaveOrJoinResult.id == 0)
                    {
                        if(connectionIdentitis.TryGetFirstValue(leaveOrJoinResult.connection, out identity, out connectionIterator))
                        {
                            do
                            {
                                if(identity.channel == leaveOrJoinResult.channel)
                                {
                                    connectionIdentitis.Remove(connectionIterator);

                                    break;
                                }
                            } while (connectionIdentitis.TryGetNextValue(out identity, ref connectionIterator));
                        }

                        if (channelConnections.TryGetFirstValue(leaveOrJoinResult.channel, out targetConnection, out channelIterator))
                        {
                            do
                            {
                                if (targetConnection == leaveOrJoinResult.connection)
                                {
                                    channelConnections.Remove(channelIterator);

                                    break;
                                }
                            } while (channelConnections.TryGetNextValue(out targetConnection, ref channelIterator));
                        }
                    }
                    else
                    {
                        isContains = false;
                        if (connectionIdentitis.TryGetFirstValue(leaveOrJoinResult.connection, out identity, out connectionIterator))
                        {
                            do
                            {
                                if (identity.channel == leaveOrJoinResult.channel)
                                {
                                    identity.id = leaveOrJoinResult.id;
                                    identity.name = leaveOrJoinResult.stream.ReadFixedString32();

                                    connectionIdentitis.SetValue(identity, connectionIterator);

                                    isContains = true;

                                    break;
                                }
                            } while (connectionIdentitis.TryGetNextValue(out identity, ref connectionIterator));
                        }

                        if (!isContains)
                        {
                            identity.channel = leaveOrJoinResult.channel;
                            identity.id = leaveOrJoinResult.id;
                            identity.name = leaveOrJoinResult.stream.ReadFixedString32();
                            connectionIdentitis.Add(leaveOrJoinResult.connection, identity);

                            channelConnections.Add(leaveOrJoinResult.channel, leaveOrJoinResult.connection);
                        }
                    }
                }
            }
        }

        [BurstCompile]
        private struct SendTalkingResults : IJobParallelForDefer
        {
            public StreamCompressionModel model;

            public NetworkDriver.Concurrent driver;

            [ReadOnly]
            public NativeArray<NetworkConnection> connections;

            [ReadOnly]
            public NativeParallelMultiHashMap<NetworkConnection, Identity> connectionIdentitis;

            [ReadOnly]
            public NativeParallelMultiHashMap<NetworkConnection, Result> talkingResults;

            public void Execute(int index)
            {
                bool isContains;
                int messageSize, result;
                StatusCode statusCode;
                DataStreamWriter writer = default;
                var connection = connections[index];
                NativeList<byte> bytes = default;
                NativeArray<byte> temp;
                foreach(var talkingResult in talkingResults.GetValuesForKey(connection))
                {
                    messageSize = talkingResult.stream.Length - talkingResult.stream.GetBytesRead();
                    if (writer.IsCreated && writer.Capacity - writer.Length < messageSize + (UnsafeUtility.SizeOf<uint>() << 1))
                    {
                        result = driver.EndSend(writer);
                        if (result < 0)
                        {
                            statusCode = (StatusCode)result;

                            if (StatusCode.Success != statusCode)
                                __LogError(statusCode);
                        }

                        writer = default;
                    }

                    if (!writer.IsCreated)
                    {
                        statusCode = (StatusCode)driver.BeginSend(connection, out writer);
                        if (StatusCode.Success != statusCode)
                        {
                            __LogError(statusCode);

                            break;
                        }
                    }

                    writer.WritePackedUInt((uint)messageSize, model);
                    writer.WritePackedUInt(talkingResult.id, model);
                    writer.WritePackedULong(talkingResult.channel, model);

                    isContains = false;
                    foreach (var identity in connectionIdentitis.GetValuesForKey(talkingResult.connection))
                    {
                        if(identity.channel == talkingResult.channel)
                        {
                            writer.WriteFixedString32(identity.name);

                            isContains = true;

                            break;
                        }
                    }

                    if (!isContains)
                        writer.WriteFixedString32(default);

                    if (!bytes.IsCreated)
                        bytes = new NativeList<byte>(Allocator.Temp);

                    bytes.ResizeUninitialized(messageSize);
                    temp = bytes.AsArray();
                    talkingResult.stream.ReadBytes(temp);
                    writer.WriteBytes(temp);
                }

                if (bytes.IsCreated)
                    bytes.Dispose();

                if (writer.IsCreated)
                {
                    result = driver.EndSend(writer);
                    if (result < 0)
                    {
                        statusCode = (StatusCode)result;

                        if (StatusCode.Success != statusCode)
                            __LogError(statusCode);
                    }
                }
            }

            private void __LogError(StatusCode statusCode)
            {
                UnityEngine.Debug.LogError($"SendTalkingResults: {(int)statusCode}");
            }
        }

        private NetworkDriver __driver;

        private NativeBuffer __buffer;

        private NativeList<NetworkConnection> __connections;

        private NativeParallelMultiHashMap<NetworkConnection, Identity> __connectionIdentitis;

        private NativeParallelMultiHashMap<ulong, NetworkConnection> __channelConnections;

        private NativeParallelMultiHashMap<NetworkConnection, Result> __talkingResults;

        private NativeQueue<Result> __leaveOrJoinResults;

        public bool isListening => __driver.Listening;

        public NetworkChatServer(in AllocatorManager.AllocatorHandle allocator)
        {
            __driver = NetworkDriver.Create();
            __buffer = new NativeBuffer(allocator, 1);
            __connections = new NativeList<NetworkConnection>(allocator);
            __connectionIdentitis = new NativeParallelMultiHashMap<NetworkConnection, Identity>(1, allocator);
            __channelConnections = new NativeParallelMultiHashMap<ulong, NetworkConnection>(1, allocator);
            __talkingResults = new NativeParallelMultiHashMap<NetworkConnection, Result>(1, allocator);
            __leaveOrJoinResults = new NativeQueue<Result>(allocator);
        }

        public void Dispose()
        {
            __driver.Dispose();
            __buffer.Dispose();
            __connections.Dispose();
            __connectionIdentitis.Dispose();
            __channelConnections.Dispose();
            __talkingResults.Dispose();
            __leaveOrJoinResults.Dispose();
        }

        public void Listen(ushort port, NetworkFamily family = NetworkFamily.Ipv4)
        {
            NetworkEndpoint endpoint;
            switch (family)
            {
                case NetworkFamily.Ipv4:
                    endpoint = NetworkEndpoint.AnyIpv4;// The local address to which the client will connect to is 127.0.0.1
                    break;
                case NetworkFamily.Ipv6:
                    endpoint = NetworkEndpoint.AnyIpv6;
                    break;
                default:
                    endpoint = default;
                    break;
            }

            endpoint.Port = port;
            if (__driver.Bind(endpoint) != 0 || __driver.Listen() != 0)
                UnityEngine.Debug.LogError($"Failed to bind to port {port}");
        }

        public JobHandle ScheduleUpdate(int innerloopBatchCount, in JobHandle inputDeps)
        {
            var driver = __driver.ToConcurrent();

            var jobHandle = __driver.ScheduleUpdate(inputDeps);

            AcceptConnections acceptConnections;
            acceptConnections.driver = __driver;
            acceptConnections.buffer = __buffer;
            acceptConnections.connections = __connections;
            acceptConnections.talkingResults = __talkingResults;
            jobHandle = acceptConnections.Schedule(jobHandle);

            var connections = __connections.AsDeferredJobArray();

            PopEvents popEvents;
            popEvents.model = StreamCompressionModel.Default;
            popEvents.driver = driver;
            popEvents.buffer = __buffer.parallelWriter;
            popEvents.connections = connections;
            popEvents.connectionIdentitis = __connectionIdentitis;
            popEvents.channelConnections = __channelConnections;
            popEvents.talkingResults = __talkingResults.AsParallelWriter();
            popEvents.leaveOrJoinResults = __leaveOrJoinResults.AsParallelWriter();

            jobHandle = popEvents.ScheduleByRef(__connections, innerloopBatchCount, jobHandle);

            ApplyLeaveOrJoinResults applyLeaveOrJoinResults;
            applyLeaveOrJoinResults.driver = __driver;
            applyLeaveOrJoinResults.connections = __connections;
            applyLeaveOrJoinResults.leaveOrJoinResults = __leaveOrJoinResults;
            applyLeaveOrJoinResults.connectionIdentitis = __connectionIdentitis;
            applyLeaveOrJoinResults.channelConnections = __channelConnections;

            jobHandle = applyLeaveOrJoinResults.Schedule(jobHandle);

            SendTalkingResults sendTalkingResults;
            sendTalkingResults.model = StreamCompressionModel.Default;
            sendTalkingResults.driver = driver;
            sendTalkingResults.connections = connections;
            sendTalkingResults.connectionIdentitis = __connectionIdentitis;
            sendTalkingResults.talkingResults = __talkingResults;

            jobHandle = sendTalkingResults.ScheduleByRef(__connections, innerloopBatchCount, jobHandle);

            return jobHandle;
        }
    }

    [AutoCreateIn("Server")]
    public partial class NetworkChatServerSystem : SystemBase
    {
        public int innerloopBatchCount = 4;

        private NetworkChatServer __server;

        public bool isListening => __server.isListening;

        public void Listen(ushort port, NetworkFamily family = NetworkFamily.Ipv4)
        {
            CompleteDependency();

            __server.Listen(port, family);
        }

        protected override void OnCreate()
        {
            base.OnCreate();

            __server = new NetworkChatServer(Allocator.Persistent);
        }

        protected override void OnDestroy()
        {
            __server.Dispose();

            base.OnDestroy();
        }

        protected override void OnUpdate()
        {
            Dependency = __server.ScheduleUpdate(innerloopBatchCount, Dependency);
        }
    }
}
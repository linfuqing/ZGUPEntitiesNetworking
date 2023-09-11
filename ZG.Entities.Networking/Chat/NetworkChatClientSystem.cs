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
    public struct NetworkChatClient
    {
        private struct Identity
        {
            public uint id;
            public FixedString32Bytes name;
        }

        public struct Message : System.IComparable<Message>
        {
            public int index;
            public uint id;
            public FixedString32Bytes name;

            internal UnsafeBlock _block;

            public DataStreamReader stream => new DataStreamReader(_block.AsArray<byte>());

            public int CompareTo(Message other)
            {
                return other.index.CompareTo(other.index);
            }
        }

        [BurstCompile]
        private struct PopEvents : IJob
        {
            public StreamCompressionModel model;
            public NetworkEndpoint endPoint;
            public NetworkDriver driver;
            public NativeBuffer buffer;
            public NativeArray<NetworkConnection> connections;
            [ReadOnly]
            public NativeParallelHashMap<ulong, Identity> identities;
            public NativeParallelMultiHashMap<ulong, Message> messages;

            public void Execute()
            {
                buffer.Reset();
                messages.Clear();

                Message message;
                message.index = 0;

                var connection = connections[0];
                DataStreamReader stream;
                NetworkEvent.Type cmd;
                while (true)
                {
                    cmd = driver.PopEventForConnection(connection, out stream);
                    switch (cmd)
                    {
                        case NetworkEvent.Type.Empty:
                            return;
                        case NetworkEvent.Type.Data:
                            int messageSize;
                            ulong channel;
                            do
                            {
                                messageSize = (int)stream.ReadPackedUInt(model);
                                message.id = stream.ReadPackedUInt(model);
                                channel = stream.ReadPackedULong(model);
                                message.name = stream.ReadFixedString32();
                                message._block = buffer.writer.WriteBlock(messageSize, false);
                                stream.ReadBytes(message._block.AsArray<byte>());

                                messages.Add(channel, message);

                                ++message.index;
                            }while (stream.GetBytesRead() < stream.Length);
                            break;
                        case NetworkEvent.Type.Connect:
                            messageSize = (UnsafeUtility.SizeOf<uint>() << 1) + UnsafeUtility.SizeOf<ulong>() + UnsafeUtility.SizeOf<ushort>();

                            int result;
                            StatusCode statusCode;
                            DataStreamWriter writer = default;
                            Identity identity;
                            foreach (var pair in identities)
                            {
                                identity = pair.Value;
                                if (writer.IsCreated && writer.Capacity - writer.Length < messageSize + identity.name.Length)
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

                                writer.WritePackedUInt((uint)NetworkChatMessageType.Join, model);
                                writer.WritePackedUInt(identity.id, model);
                                writer.WritePackedULong(pair.Key, model);
                                writer.WriteFixedString32(identity.name);
                            }

                            if(writer.IsCreated)
                            {
                                result = driver.EndSend(writer);
                                if (result < 0)
                                {
                                    statusCode = (StatusCode)result;

                                    if (StatusCode.Success != statusCode)
                                        __LogError(statusCode);
                                }
                            }
                            break;
                        case NetworkEvent.Type.Disconnect:
                            __LogDisconnectReason((DisconnectReason)stream.ReadByte());

                            connections[0] = driver.Connect(endPoint);
                            return;
                    }
                }
            }

            private void __LogError(StatusCode statusCode)
            {
                UnityEngine.Debug.LogError($"Join: {(int)statusCode}");
            }

            private void __LogDisconnectReason(DisconnectReason disconnectReason)
            {
                UnityEngine.Debug.LogError($"DisconnectReason: {(int)disconnectReason}");
            }
        }

        private NetworkDriver __driver;
        private NativeBuffer __buffer;
        private NativeArray<NetworkEndpoint> __endPoints;
        private NativeArray<NetworkConnection> __connections;
        private NativeParallelHashMap<ulong, Identity> __identities;
        private NativeParallelMultiHashMap<ulong, Message> __messages;

        public NetworkConnection.State connectionState => __driver.GetConnectionState(connection);

        public NetworkConnection connection
        {
            get => __connections[0];

            private set => __connections[0] = value;
        }

        public NetworkChatClient(in AllocatorManager.AllocatorHandle allocator)
        {
            __driver = NetworkDriver.Create();
            __buffer = new NativeBuffer(allocator, 1);
            __endPoints = CollectionHelper.CreateNativeArray<NetworkEndpoint>(1, allocator, NativeArrayOptions.ClearMemory);
            __connections = CollectionHelper.CreateNativeArray<NetworkConnection>(1, allocator, NativeArrayOptions.ClearMemory);
            __identities = new NativeParallelHashMap<ulong, Identity>(1, allocator);
            __messages = new NativeParallelMultiHashMap<ulong, Message>(1, allocator);
        }

        public void Dispose()
        {
            __driver.Dispose();
            __buffer.Dispose();
            __endPoints.Dispose();
            __connections.Dispose();
            __identities.Dispose();
            __messages.Dispose();
        }

        public void Shutdown()
        {
            __driver.Disconnect(connection);

            __identities.Clear();
        }

        public void Connect(in NetworkEndpoint endPoint)
        {
            if (NetworkConnection.State.Disconnected != connectionState)
                __driver.Disconnect(connection);

            __endPoints[0] = endPoint;

            connection = __driver.Connect(endPoint);
        }

        public int Leave(ulong channel)
        {
            if (!__identities.ContainsKey(channel))
                return -1;

            if (NetworkConnection.State.Connected == connectionState)
            {
                int result = __driver.BeginSend(connection, out var writer);
                if (StatusCode.Success == (StatusCode)result)
                {
                    writer.WritePackedUInt((uint)NetworkChatMessageType.Leave);
                    writer.WritePackedULong(channel);

                    result = __driver.EndSend(writer);
                    if (result >= 0)
                        __identities.Remove(channel);
                }

                return result;
            }

            __identities.Remove(channel);

            return 0;
        }

        public int Join(uint id, ulong channel, in FixedString32Bytes name)
        {
            Identity identity;
            identity.id = id;
            identity.name = name;
            if (NetworkConnection.State.Connected == connectionState)
            {
                int result = __driver.BeginSend(connection, out var writer);
                if (StatusCode.Success == (StatusCode)result)
                {
                    writer.WritePackedUInt((uint)NetworkChatMessageType.Join);
                    writer.WritePackedUInt(id);
                    writer.WritePackedULong(channel);
                    writer.WriteFixedString32(name);

                    result = __driver.EndSend(writer);
                    if (result >= 0)
                        __identities[channel] = identity;
                }

                return result;
            }

            __identities[channel] = identity;

            return 0;
        }

        public StatusCode BeginTalk(ulong channel, out DataStreamWriter writer)
        {
            StatusCode statusCode = (StatusCode)__driver.BeginSend(connection, out writer);
            if (StatusCode.Success == statusCode)
            {
                writer.WritePackedUInt((uint)NetworkChatMessageType.Talk);
                writer.WritePackedULong(channel);
            }

            return statusCode;
        }

        public int EndTalk(DataStreamWriter writer)
        {
            return __driver.EndSend(writer);
        }

        public StatusCode BeginWhisper(uint id, ulong channel, out DataStreamWriter writer)
        {
            StatusCode statusCode = (StatusCode)__driver.BeginSend(connection, out writer);
            if (StatusCode.Success == statusCode)
            {
                writer.WritePackedUInt((uint)NetworkChatMessageType.Whisper);
                writer.WritePackedUInt(id);
                writer.WritePackedULong(channel);
            }

            return statusCode;
        }

        public int EndWhisper(DataStreamWriter writer)
        {
            return __driver.EndSend(writer);
        }

        public void GetChannels(ref NativeList<ulong> channels)
        {
            ulong channel;
            foreach(var message in __messages)
            {
                channel = message.Key;
                if (channels.IndexOf(channel) != -1)
                    continue;

                channels.Add(channel);
            }
        }

        public void GetMessages(ulong channel, ref NativeList<Message> messages)
        {
            foreach (var message in __messages.GetValuesForKey(channel))
                messages.Add(message);

            messages.Sort();
        }

        public JobHandle ScheduleUpdate(in JobHandle inputDeps)
        {
            var jobHandle = __driver.ScheduleUpdate(inputDeps);

            PopEvents popEvents;
            popEvents.model = StreamCompressionModel.Default;
            popEvents.endPoint = __endPoints[0];
            popEvents.driver = __driver;
            popEvents.buffer = __buffer;
            popEvents.connections = __connections;
            popEvents.identities = __identities;
            popEvents.messages = __messages;

            return popEvents.Schedule(jobHandle);
        }
    }

    public struct NetworkChatClientManager : IComponentData
    {
        private UnsafeList<LookupJobManager> __lookupJobManager;

        public bool isCreated => __lookupJobManager.IsCreated;

        public ref LookupJobManager lookupJobManager => ref __lookupJobManager.ElementAt(0);

        public NetworkChatClient client
        {
            get;
        }

        public static EntityQuery GetEntityQuery(ref SystemState state)
        {
            using (var builder = new EntityQueryBuilder(Allocator.Temp))
                return builder
                    .WithAll<NetworkChatClientManager>()
                    .WithOptions(EntityQueryOptions.IncludeSystems)
                    .Build(ref state);
        }

        public NetworkChatClientManager(in AllocatorManager.AllocatorHandle allocator)
        {
            __lookupJobManager = new UnsafeList<LookupJobManager>(1, allocator, NativeArrayOptions.UninitializedMemory);
            __lookupJobManager.Resize(1, NativeArrayOptions.ClearMemory);

            client = new NetworkChatClient(allocator);
        }

        public void Dispose()
        {
            lookupJobManager.CompleteReadWriteDependency();

            __lookupJobManager.Dispose();

            client.Dispose();
        }

        public JobHandle Update(in JobHandle inputDeps)
        {
            var jobHandle = JobHandle.CombineDependencies(lookupJobManager.readWriteJobHandle, inputDeps);
            jobHandle = client.ScheduleUpdate(jobHandle);

            lookupJobManager.readWriteJobHandle = jobHandle;

            return jobHandle;
        }
    }


    [AutoCreateIn("Client"), BurstCompile]
    public partial struct NetworkChatClientSystem : ISystem
    {
        public NetworkChatClientManager client
        {
            get;

            private set;
        }

        //[BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            client = new NetworkChatClientManager(Allocator.Persistent);
        }

        //[BurstCompile]
        public void OnDestroy(ref SystemState state)
        {
            client.Dispose();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            state.Dependency = client.Update(state.Dependency);
        }
    }
}
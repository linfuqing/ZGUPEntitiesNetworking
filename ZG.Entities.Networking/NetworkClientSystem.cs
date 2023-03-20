using Unity.Jobs;
using Unity.Burst;
using Unity.Entities;
using Unity.Collections;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;
using System.Diagnostics;

namespace ZG
{
    /*public struct NetworkMessage : IBufferElementData
    {
        public DataStreamReader reader;
    }*/

    public struct NetworkClient
    {
        private struct Version
        {
            public uint value;

#if DEBUG
            public bool isActive;
#endif
        }

        private struct RPCBuffer
        {
            public uint version;

            public UnsafeBuffer value;
        }

        public struct Message
        {
            public uint type;

            internal UnsafeBlock _block;

            public DataStreamReader stream => _block.isCreated ? new DataStreamReader(_block.AsArray<byte>()) : default;
        }

        public struct Buffer : System.IDisposable
        {
            private uint __id;
            private NativeHashMap<uint, UnsafeBuffer> __buffers;
            private NativeList<UnsafeBuffer> __bufferPool;

            public bool isCreated => __buffers.IsCreated && __buffers.ContainsKey(__id);

            internal Buffer(uint id, NativeHashMap<uint, UnsafeBuffer> buffers, NativeList<UnsafeBuffer> bufferPool)
            {
                __id = id;
                __buffers = buffers;
                __bufferPool = bufferPool;
            }

            public NetworkMessageType ReadMessage(out DataStreamReader stream, out uint identity)
            {
                if (__buffers.IsCreated && 
                    __buffers.TryGetValue(__id, out var buffer) && 
                    buffer.position < buffer.length)
                {
                    var reader = buffer.reader;
                    var messageType = (NetworkMessageType)reader.Read<byte>();
                    identity = messageType == NetworkMessageType.Register ? reader.Read<uint>() : 0;
                    int length = reader.Read<int>();
                    stream = new DataStreamReader(reader.ReadArray<byte>(length));

                    __buffers[__id] = buffer;

                    return messageType;
                }
                else
                    identity = 0;

                stream = default;

                return NetworkMessageType.Unknown;
            }

            public void Dispose()
            {
                if (__buffers.IsCreated && 
                    __buffers.TryGetValue(__id, out var buffer) &&
                    buffer.position == buffer.length)
                {
                    __bufferPool.Add(buffer);

                    __buffers.Remove(__id);
                }
            }
        }

        [BurstCompile]
        private struct PopEvents : IJob
        {
            public NativeArray<NetworkConnection> connections;

            public NetworkDriver driver;

            public NativeBuffer buffer;

            public StreamCompressionModel model;
            public NativeList<Message> messages;

            public NativeList<UnsafeBuffer> bufferPool;

            public NativeHashMap<uint, Version> versions;

            public NativeHashMap<uint, UnsafeBuffer> buffers;

            public NativeParallelMultiHashMap<uint, RPCBuffer> rpcBuffers;

            public bool TryGetRPCBuffer(uint id, uint version, out UnsafeBuffer buffer, out NativeParallelMultiHashMapIterator<uint> iterator)
            {
                if (rpcBuffers.TryGetFirstValue(id, out var rpcBuffer, out iterator))
                {
                    do
                    {
                        if (rpcBuffer.version == version)
                        {
                            buffer = rpcBuffer.value;

                            return true;
                        }
                    } while (rpcBuffers.TryGetNextValue(out rpcBuffer, ref iterator));
                }

                buffer = default;

                return false;
            }

            public void Execute()
            {
                messages.Clear();

                //identities.Clear();

                buffer.Reset();

                var connection = connections[0];
                if (connection.IsCreated)
                {
                    var messageStream = buffer.writer;

                    DataStreamReader stream;
                    NetworkEvent.Type cmd;
                    Message message;
                    while (true)
                    {
                        cmd = driver.PopEventForConnection(connection, out stream);
                        switch (cmd)
                        {
                            case NetworkEvent.Type.Empty:
                                if (NetworkConnection.State.Disconnected == driver.GetConnectionState(connection))
                                {
                                    versions.Clear();

                                    foreach(var rpcBuffer in rpcBuffers)
                                        rpcBuffer.Value.value.Dispose();

                                    rpcBuffers.Clear();

                                    message.type = (uint)NetworkMessageType.Disconnect;

                                    message._block = messageStream.WriteBlock(1, false);

                                    message._block.As<byte>() = (byte)DisconnectReason.Default;

                                    messages.Add(message);
                                    //* disconnectReason = (DisconnectReason)stream.ReadByte();

                                    connections[0] = default;
                                }
                                return;
                            case NetworkEvent.Type.Data:
                                do
                                {
                                    uint messageType = stream.ReadPackedUInt(model);
                                    switch (messageType)
                                    {
                                        case (uint)NetworkMessageType.RPC:
                                        case (uint)NetworkMessageType.Register:
                                        case (uint)NetworkMessageType.Unregister:
                                            uint id = stream.ReadPackedUInt(model);

                                            bool isBufferRPC = false;
                                            Version version = default;
                                            if (messageType == (uint)NetworkMessageType.RPC)
                                            {
                                                version.value = stream.ReadPackedUInt(model);
                                                UnityEngine.Assertions.Assert.AreNotEqual(0, version.value);
                                                isBufferRPC = !versions.TryGetValue(id, out var originVersion) || originVersion.value < version.value;
                                            }

                                            UnsafeBuffer buffer;
                                            if (isBufferRPC)
                                            {
                                                if (TryGetRPCBuffer(id, version.value, out buffer, out var iterator))
                                                    rpcBuffers.Remove(iterator);
                                            }
                                            else if (!buffers.TryGetValue(id, out buffer))
                                                buffer = default;

                                            if(!buffer.isCreated)
                                            {
                                                int bufferPoolLength = bufferPool.Length;
                                                if (bufferPoolLength > 0)
                                                {
                                                    buffer = bufferPool[--bufferPoolLength];
                                                    buffer.Reset();

                                                    bufferPool.ResizeUninitialized(bufferPoolLength);
                                                }
                                                else
                                                    buffer = new UnsafeBuffer(0, 1, Allocator.Persistent);
                                            }

                                            var position = buffer.position;
                                            buffer.position = buffer.length;

                                            var writer = buffer.writer;

                                            writer.Write((byte)messageType);

                                            /*if (messageType == (uint)NetworkMessageType.Register)
                                                identities[id] = stream.ReadPackedUInt(model);
                                            else if (messageType == (uint)NetworkMessageType.Unregister)
                                                identities.Remove(id);*/

                                            /*if(messageType == (uint)NetworkMessageType.RPC)
                                                UnityEngine.Debug.LogError($"Revice {stream.Length}");*/

                                            if (messageType == (uint)NetworkMessageType.Register)
                                            {
#if DEBUG
                                                UnityEngine.Assertions.Assert.IsFalse(versions.TryGetValue(id, out var originVersion) && originVersion.isActive);

                                                version.isActive = true;
#endif
                                                version.value = stream.ReadPackedUInt(model);

                                                versions[id] = version;

                                                writer.Write(stream.ReadPackedUInt(model));
                                            }
#if DEBUG
                                            else if(messageType == (uint)NetworkMessageType.Unregister)
                                            {
                                                var originVersion = versions[id];

                                                UnityEngine.Assertions.Assert.IsTrue(originVersion.isActive);

                                                originVersion.isActive = false;

                                                versions[id] = originVersion;
                                            }
#endif

                                            int length = (int)stream.ReadPackedUInt(model);
                                            writer.Write(length);

                                            var block = writer.WriteBlock(length, false);
                                            //���룬����Writer.Flush
                                            stream.ReadBytes(block.AsArray<byte>());

                                            if (isBufferRPC)
                                            {
                                                RPCBuffer rpcBuffer;
                                                rpcBuffer.version = version.value;
                                                rpcBuffer.value = buffer;
                                                rpcBuffers.Add(id, rpcBuffer);
                                            }
                                            else
                                            {
                                                if (messageType == (uint)NetworkMessageType.Register)
                                                {
                                                    if (TryGetRPCBuffer(id, version.value, out var rpcBuffer, out var iterator))
                                                    {
                                                        writer.WriteBuffer(rpcBuffer);

                                                        bufferPool.Add(rpcBuffer);

                                                        rpcBuffers.Remove(iterator);
                                                    }
                                                }

                                                buffer.position = position;

                                                buffers[id] = buffer;
                                            }

                                            break;
                                        default:
                                            message.type = messageType;

                                            message._block = messageStream.WriteBlock(stream.ReadUShort(), false);

                                            stream.ReadBytes(message._block.AsArray<byte>());

                                            messages.Add(message);
                                            break;
                                    }
                                } while (stream.GetBytesRead() < stream.Length);
                                break;
                            case NetworkEvent.Type.Connect:
                                message.type = (uint)NetworkMessageType.Connect;

                                message._block = default;

                                messages.Add(message);
                                break;
                            case NetworkEvent.Type.Disconnect:
                                //identities.Clear();

                                versions.Clear();

                                foreach (var rpcBuffer in rpcBuffers)
                                    rpcBuffer.Value.value.Dispose();

                                rpcBuffers.Clear();

                                message.type = (uint)NetworkMessageType.Disconnect;

                                message._block = messageStream.WriteBlock(stream.Length, false);

                                stream.ReadBytes(message._block.AsArray<byte>());

                                messages.Add(message);
                                //* disconnectReason = (DisconnectReason)stream.ReadByte();

                                connections[0] = default;
                                break;
                        }
                    }
                }
            }
        }

        private NetworkDriver __driver;

        private NativeBuffer __buffer;

        private NativeArray<NetworkConnection> __connections;

        private NativeList<Message> __messages;

        private NativeList<UnsafeBuffer> __bufferPool;

        private NativeHashMap<uint, Version> __versions;

        private NativeHashMap<uint, UnsafeBuffer> __buffers;

        private NativeParallelMultiHashMap<uint, RPCBuffer> __rpcBuffers;

        public bool isCreated => __connections.IsCreated;

        public NetworkConnection connection => __connections[0];

        public NetworkConnection.State connectionState => __driver.GetConnectionState(__connections[0]);

        //public unsafe DisconnectReason disconnectReason => __data->disconnectReason;

        public NativeArray<Message>.ReadOnly messagesReadOnly => __messages.AsArray().AsReadOnly();

        public NativeHashMap<uint, UnsafeBuffer> buffers => __buffers;

        public NetworkClient(Allocator allocator)
        {
            __driver = NetworkDriver.Create();

            __buffer = new NativeBuffer(allocator, 1);

            __connections = new NativeArray<NetworkConnection>(1, allocator, NativeArrayOptions.ClearMemory);

            __messages = new NativeList<Message>(allocator);

            __bufferPool = new NativeList<UnsafeBuffer>(allocator);

            __versions = new NativeHashMap<uint, Version>(1, allocator);

            __buffers = new NativeHashMap<uint, UnsafeBuffer>(1, allocator);

            __rpcBuffers = new NativeParallelMultiHashMap<uint, RPCBuffer>(1, allocator);

        }

        public void Dispose()
        {
            __driver.Dispose();

            __buffer.Dispose();

            __connections.Dispose();

            __messages.Dispose();

            foreach (var buffer in __bufferPool)
                buffer.Dispose();

            __bufferPool.Dispose();

            __versions.Dispose();

            foreach (var buffer in __buffers)
                buffer.Value.Dispose();

            __buffers.Dispose();

            foreach (var rpcBuffer in __rpcBuffers)
                rpcBuffer.Value.value.Dispose();

            __rpcBuffers.Dispose();
        }

        public void Shutdown()
        {
            foreach (var rpcBuffer in __rpcBuffers)
                __bufferPool.Add(rpcBuffer.Value.value);

            __rpcBuffers.Clear();

            foreach (var buffer in __buffers)
                __bufferPool.Add(buffer.Value);

            __buffers.Clear();

            __versions.Clear();

            __messages.Clear();

            __driver.Disconnect(__connections[0]);
        }

        public void Connect(in NetworkEndpoint endPoint)
        {
            __connections[0] = __driver.Connect(endPoint);
        }

        public NetworkPipeline CreatePipeline(NetworkPipelineType type)
        {
            return __driver.CreatePipeline(type);
        }

        public StatusCode BeginSendRegister(in NetworkPipeline pipeline, out DataStreamWriter writer)
        {
            return BeginSend(pipeline, (uint)NetworkMessageType.Register, out writer);
        }

        public StatusCode BeginSendUnregister(in NetworkPipeline pipeline, out DataStreamWriter writer)
        {
            return BeginSend(pipeline, (uint)NetworkMessageType.Unregister, out writer);
        }

        public StatusCode BeginSendRPC(in NetworkPipeline pipeline, uint id, out DataStreamWriter writer)
        {
            var statusCode = (StatusCode)__driver.BeginSend(pipeline, connection, out writer);
            if (statusCode == StatusCode.Success)
            {
                writer.WritePackedUInt((uint)NetworkMessageType.RPC);
                writer.WritePackedUInt(id);
                writer.Flush();
            }

            return statusCode;
        }

        public StatusCode BeginSend(in NetworkPipeline pipeline, uint messageType, out DataStreamWriter writer)
        {
            var statusCode = (StatusCode)__driver.BeginSend(pipeline, connection, out writer);
            if (statusCode == StatusCode.Success)
            {
                writer.WritePackedUInt(messageType);
                writer.Flush();
            }

            return statusCode;
        }

        public int EndSend(in DataStreamWriter writer)
        {
            //lookupJobManager.CompleteReadWriteDependency();

            return __driver.EndSend(writer);
        }

        public void AbortSend(in DataStreamWriter writer)
        {
            //lookupJobManager.CompleteReadWriteDependency();

            __driver.AbortSend(writer);
        }

        public void GetIDs(ref NativeList<uint> ids)
        {
            var enumerator = __buffers.GetEnumerator();
            while (enumerator.MoveNext())
                ids.Add(enumerator.Current.Key);
        }

        public Buffer Receive(uint id)
        {
            return new Buffer(id, __buffers, __bufferPool);
        }

        public JobHandle ScheduleUpdate(in JobHandle inputDeps)
        {
            var jobHandle = __driver.ScheduleUpdate(inputDeps);

            PopEvents popEvents;
            //popEvents.disconnectReason = &__data->disconnectReason;
            popEvents.connections = __connections;
            popEvents.driver = __driver;
            popEvents.buffer = __buffer;
            popEvents.model = StreamCompressionModel.Default;
            popEvents.messages = __messages;
            popEvents.bufferPool = __bufferPool;
            popEvents.versions = __versions;
            popEvents.buffers = __buffers;
            popEvents.rpcBuffers = __rpcBuffers;

            return popEvents.Schedule(jobHandle);
        }
    }

    [AutoCreateIn("Client")]
    public partial class NetworkClientSystem : SystemBase
    {
        public LookupJobManager lookupJobManager;

        public NetworkClient client
        {
            get;

            private set;
        }

        public NetworkClientSystem()
        {
            client = new NetworkClient(Allocator.Persistent);
        }

        protected override void OnDestroy()
        {
            lookupJobManager.CompleteReadWriteDependency();

            client.Dispose();

            base.OnDestroy();
        }

        protected override void OnUpdate()
        {
            var jobHandle = JobHandle.CombineDependencies(lookupJobManager.readWriteJobHandle, Dependency);

            jobHandle = client.ScheduleUpdate(jobHandle);

            lookupJobManager.readWriteJobHandle = jobHandle;

            Dependency = jobHandle;
        }
    }
}